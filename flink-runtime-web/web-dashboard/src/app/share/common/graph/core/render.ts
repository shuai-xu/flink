/**
 * ==============================================================================
 * This product contains a modified version of 'TensorBoard plugin for graphs',
 * a Angular implementation of nest-graph visualization
 *
 * Copyright 2018 The flink-runtime-web Authors. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the 'License');
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ==============================================================================
 */

import { scaleLinear, ScaleLinear, ScalePower } from 'd3-scale';
import { Selection } from 'd3-selection';
import { runAsyncTask } from './util';
import { MAX_EDGE_WIDTH, MIN_EDGE_WIDTH } from './edge';
import { createGraph } from './graph';
import { Hierarchy } from './hierarchy';
import { BridgeNode, GraphType, GroupNode, InclusionType, Metaedge, NAMESPACE_DELIM, Node, NodeType } from './interface';
import GraphOptions = graphlib.GraphOptions;


const PARAMS = {
  maxBridgePathDegree: 4
};

export interface Point {
  x: number;
  y: number;
}

/**
 * Function that computes edge thickness in pixels.
 */
export type EdgeThicknessFunction = (edgeData: any, edgeClass: string) => number;

/**
 * Function that computes edge label strings. This function accepts a Metaedge,
 * which could actually encapsulate several base edges. For instance, several
 * base edges may merge into a single metaedge.
 *
 * To determine whether a metaedge represents several edges, check the length of
 * its baseEdgeList property.
 */
export type EdgeLabelFunction = (metaedge: Metaedge, renderInfo: RenderGraphInfo) => string;

export class RenderGraphInfo {
  hierarchy: Hierarchy;
  private displayingStats: boolean;
  private index: { [ nodeName: string ]: RenderNodeInfo };
  private renderedOpNames: string[];

  /** Scale for the thickness of edges when there is no shape information. */
  edgeWidthSizedBasedScale:
    ScaleLinear<number, number> | ScalePower<number, number>;
  // Since the rendering information for each node is constructed lazily,
  // upon node's expansion by the user, we keep a map between the node's name
  // and whether the rendering information was already constructed for that
  // node.
  private hasSubhierarchy: { [ nodeName: string ]: boolean };
  root: RenderGroupNodeInfo;
  traceInputs: Boolean;
  edgeLabelFunction: EdgeLabelFunction;
  // An optional function that computes the thickness of an edge given edge
  // data. If not provided, defaults to encoding tensor size in thickness.
  edgeWidthFunction: EdgeThicknessFunction;

  constructor(hierarchy: Hierarchy, displayingStats: boolean) {
    this.hierarchy = hierarchy;
    this.displayingStats = displayingStats;
    this.index = {};
    this.renderedOpNames = [];

    this.computeScales();

    this.hasSubhierarchy = {};
    this.root = new RenderGroupNodeInfo(hierarchy.root, hierarchy.graphOptions);
    this.index[ hierarchy.root.name ] = this.root;
    this.renderedOpNames.push(hierarchy.root.name);
    this.buildSubhierarchy(hierarchy.root.name);
    this.root.expanded = true;
    this.traceInputs = true;
  }

  computeScales(): void {

    this.edgeWidthSizedBasedScale = scaleLinear()
    .domain([ 1, this.hierarchy.maxMetaEdgeSize ])
    .range([ MIN_EDGE_WIDTH, MAX_EDGE_WIDTH ] as ReadonlyArray<number>);
  }

  getSubhierarchy() {
    return this.hasSubhierarchy;
  }

  buildSubhierarchy(nodeName: string): void {
    if (nodeName in this.hasSubhierarchy) {
      return;
    }
    this.hasSubhierarchy[ nodeName ] = true;

    const renderNodeInfo = this.index[ nodeName ];

    if (renderNodeInfo.node.type !== NodeType.META &&
      renderNodeInfo.node.type !== NodeType.SERIES) {
      return;
    }

    const renderGroupNodeInfo = <RenderGroupNodeInfo> renderNodeInfo;

    const metagraph = renderGroupNodeInfo.node.metagraph;
    const coreGraph = renderGroupNodeInfo.coreGraph;

    metagraph.nodes().forEach(childName => {
      const childRenderInfo = this.getOrCreateRenderNodeByName(childName);
      coreGraph.setNode(childName, childRenderInfo);
    });

    metagraph.edges().forEach(edgeObj => {
      const metaedge = metagraph.edge(edgeObj);
      const renderMetaedgeInfo = new RenderMetaedgeInfo(metaedge);
      coreGraph.setEdge(edgeObj.v, edgeObj.w, renderMetaedgeInfo);
    });

    const parentNode = renderGroupNodeInfo.node.parentNode;
    if (!parentNode) {
      return;
    }
    const parentNodeInfo = <RenderGroupNodeInfo> this.index[ parentNode.name ];

    const getBridgeNodeName = (inbound, ...rest) =>
      rest.concat([ inbound ? 'IN' : 'OUT' ]).join('~~');

    const bridgegraph = this.hierarchy.getBridgegraph(nodeName);
    // Look for popular nodes so we can make annotations instead of paths.
    const otherCounts = {
      // Counts of edges coming INTO other nodes by name (outgoing from self).
      in     : <{ [ nodeName: string ]: number }> {},
      // Counts of edges going OUT from other nodes by name (coming into self).
      out    : <{ [ nodeName: string ]: number }> {},
      // Counts of all control edges involving other nodes by name.
      control: <{ [ nodeName: string ]: number }> {}
    };

    bridgegraph.edges().forEach(e => {
      // An edge is inbound if its destination node is in the metagraph.
      const inbound = !!metagraph.node(e.w);
      const otherName = inbound ? e.v : e.w;
      const metaedge = bridgegraph.edge(e);
      if (!metaedge.numRegularEdges) {
        otherCounts.control[ otherName ] =
          (otherCounts.control[ otherName ] || 0) + 1;
      } else if (inbound) {
        otherCounts.out[ otherName ] = (otherCounts.out[ otherName ] || 0) + 1;
      } else {
        otherCounts.in[ otherName ] = (otherCounts.in[ otherName ] || 0) + 1;
      }
    });

    const hierarchyNodeMap = this.hierarchy.getNodeMap();
    bridgegraph.edges().forEach(bridgeEdgeObj => {
      const bridgeMetaedge = bridgegraph.edge(bridgeEdgeObj);
      // Determine whether this bridge edge is incoming by checking the
      // metagraph for a node that matches the destination end.
      const inbound = !!metagraph.node(bridgeEdgeObj.w);

      // Based on the direction of the edge, one endpoint will be an immediate
      // child of this renderNodeInfo, and the other endpoint will be a sibling
      // of the parent (or an ancestor further up).
      const [ childName, otherName ] =
              inbound ?
                [ bridgeEdgeObj.w, bridgeEdgeObj.v ] :
                [ bridgeEdgeObj.v, bridgeEdgeObj.w ];

      const childRenderInfo = this.index[ childName ];
      const otherRenderInfo = this.index[ otherName ];

      // Don't render a bridge path if the other node has in or out degree above
      // a threshold, lest bridge paths emanating out of a metagraph crowd up,
      // as was the case for the Fatcat LSTM lstm_1 > lstm_1 metagraph.
      const otherDegreeCount =
              (inbound ? otherCounts.out : otherCounts.in)[ otherName ];
      const isOtherHighDegree = otherDegreeCount > PARAMS.maxBridgePathDegree;

      // The adjoining render metaedge info from the parent's coreGraph, if any.
      // It will either be a Metaedge involving this node directly, if it
      // previously came from a metagraph, or it'll be a Metaedge involving
      // a previously created bridge node standing in for the other node.
      let adjoiningMetaedge = null;

      // We can only hope to render a bridge path if:
      //  - bridgegraph paths are enabled,
      //  - the other node is not too high-degree,
      //  - the child is in the core (not extracted for being high-degree), and
      //  - there's a path (in the traversal sense) between child and other.
      let canDrawBridgePath = false;
      if (!isOtherHighDegree &&
        childRenderInfo.isInCore()) {

        // Utility function for finding an adjoining metaedge.
        const findAdjoiningMetaedge = targetName => {
          const adjoiningEdgeObj: graphlib.EdgeObject =
                  inbound ?
                    { v: targetName, w: nodeName } :
                    { v: nodeName, w: targetName };
          return <RenderMetaedgeInfo>
            parentNodeInfo.coreGraph.edge(adjoiningEdgeObj);
        };

        adjoiningMetaedge = findAdjoiningMetaedge(otherName);
        if (!adjoiningMetaedge) {
          adjoiningMetaedge = findAdjoiningMetaedge(
            getBridgeNodeName(inbound, otherName, parentNode.name));
        }

        canDrawBridgePath = !!adjoiningMetaedge;
      }

      const backwards = false;
      if (adjoiningMetaedge && !bridgeMetaedge.numRegularEdges) {
        // Find the top-most adjoining render metaedge information, and the
        // GroupNode whose metagraph must contain the associated metaedge.
        let topAdjoiningMetaedge = adjoiningMetaedge;
        let topGroupNode = parentNodeInfo.node;
        while (topAdjoiningMetaedge.adjoiningMetaedge) {
          topAdjoiningMetaedge = topAdjoiningMetaedge.adjoiningMetaedge;
          topGroupNode = <GroupNode>topGroupNode.parentNode;
        }
        // TODO is backwards
      }

      const bridgeContainerName = getBridgeNodeName(inbound, nodeName);
      const bridgeNodeName = getBridgeNodeName(inbound, otherName, nodeName);
      let bridgeNodeRenderInfo = coreGraph.node(bridgeNodeName);

      if (!bridgeNodeRenderInfo) {
        let bridgeContainerInfo = coreGraph.node(bridgeContainerName);

        if (!bridgeContainerInfo) {
          const bridgeContainerNode: BridgeNode = {
            // Important node properties.
            name       : bridgeContainerName,
            type       : NodeType.BRIDGE,
            // Unused node properties.
            isGroupNode: false,
            cardinality: 0,
            parentNode : null,
            include    : InclusionType.UNSPECIFIED,
            // BridgeNode properties.
            inbound    : inbound,
            attr       : {}
          };
          bridgeContainerInfo =
            new RenderNodeInfo(bridgeContainerNode);
          this.index[ bridgeContainerName ] = bridgeContainerInfo;
          coreGraph.setNode(bridgeContainerName, bridgeContainerInfo);
        }


        const bridgeNode: BridgeNode = {
          // Important node properties.
          name       : bridgeNodeName,
          type       : NodeType.BRIDGE,
          // Unimportant node properties.
          isGroupNode: false,
          cardinality: 1,
          parentNode : null,
          include    : InclusionType.UNSPECIFIED,
          // BridgeNode properties.
          inbound    : inbound,
          attr       : {}
        };

        bridgeNodeRenderInfo = new RenderNodeInfo(bridgeNode);
        this.index[ bridgeNodeName ] = bridgeNodeRenderInfo;
        coreGraph.setNode(bridgeNodeName, bridgeNodeRenderInfo);

        // Set bridgeNode to be a graphlib child of the container node.
        coreGraph.setParent(bridgeNodeName, bridgeContainerName);
        bridgeContainerInfo.node.cardinality++;
      }

      // Create and add a bridge render metaedge.
      const bridgeRenderMetaedge =
              new RenderMetaedgeInfo(bridgeMetaedge);
      bridgeRenderMetaedge.adjoiningMetaedge = adjoiningMetaedge;
      inbound ?
        coreGraph.setEdge(bridgeNodeName, childName, bridgeRenderMetaedge) :
        coreGraph.setEdge(childName, bridgeNodeName, bridgeRenderMetaedge);
    });

    [ true, false ].forEach(inbound => {
      const bridgeContainerName = getBridgeNodeName(inbound, nodeName);
      const bridgeContainerInfo = coreGraph.node(bridgeContainerName);
      if (!bridgeContainerInfo) {
        return;
      }
      coreGraph.nodes().forEach(childName => {
        // Short-circuit if this child is a bridge node or it's not a terminal
        // node in the direction we're interested in.
        const childNodeInfo = coreGraph.node(childName);
        if (childNodeInfo.node.type === NodeType.BRIDGE) {
          return;
        }
        const isTerminal = inbound ?
          !coreGraph.predecessors(childName).length :
          !coreGraph.successors(childName).length;
        if (!isTerminal) {
          return;
        }

        // Find or create a bridge node in the container for all structural
        // metaedges. It would have been nice to skip this step and simply
        // set a metaedge between the terminal node and the container node, but
        // in that case, something about the graph upsets dagre.layout()'s
        // longestPath algorithm (was getting errors due to an undefined).
        const structuralNodeName =
                getBridgeNodeName(inbound, nodeName, 'STRUCTURAL_TARGET');
        let structuralRenderInfo = coreGraph.node(structuralNodeName);
        if (!structuralRenderInfo) {
          const bridgeNode: BridgeNode = {
            // Important Node properties.
            name       : structuralNodeName,
            type       : NodeType.BRIDGE,
            // Unimportant Node properties.
            isGroupNode: false,
            cardinality: 1,
            parentNode : null,
            include    : InclusionType.UNSPECIFIED,
            // BridgeNode properties.
            inbound    : inbound,
            attr       : {}
          };
          structuralRenderInfo = new RenderNodeInfo(bridgeNode);
          structuralRenderInfo.structural = true;
          this.index[ structuralNodeName ] = structuralRenderInfo;
          coreGraph.setNode(structuralNodeName, structuralRenderInfo);
          bridgeContainerInfo.node.cardinality++;
          coreGraph.setParent(structuralNodeName, bridgeContainerName);
        }

        // Create the structural Metaedge and insert it.
        const structuralMetaedgeInfo = new RenderMetaedgeInfo(null);
        structuralMetaedgeInfo.structural = true;
        structuralMetaedgeInfo.weight--; // Reduce weight for dagre layout.
        inbound ?
          coreGraph.setEdge(
            structuralNodeName, childName, structuralMetaedgeInfo) :
          coreGraph.setEdge(
            childName, structuralNodeName, structuralMetaedgeInfo);
      });
    });
  }

  getOrCreateRenderNodeByName(nodeName: string): RenderNodeInfo {
    if (!nodeName) {
      return null;
    }

    if (nodeName in this.index) {
      return this.index[ nodeName ];
    }

    const node = this.hierarchy.node(nodeName);
    if (!node) {
      return null;
    }

    this.index[ nodeName ] = node.isGroupNode ?
      new RenderGroupNodeInfo(<GroupNode>node, this.hierarchy.graphOptions) :
      new RenderNodeInfo(node);
    this.renderedOpNames.push(nodeName);

    return this.index[ nodeName ];

  }

  getRenderNodeByName(nodeName: string): RenderNodeInfo {
    return this.index[ nodeName ];
  }

  getNodeByName(nodeName: string): Node {
    return this.hierarchy.node(nodeName);
  }

}


export class RenderNodeInfo {
  node: Node;
  expanded: boolean;
  x: number;
  y: number;
  width: number;
  height: number;
  coreBox: {
    width: number,
    height: number,
  };
  inboxWidth: number;
  outboxWidth: number;
  excluded: boolean;
  structural: boolean;
  labelOffset: number;
  radius: number;
  labelHeight: number;
  paddingTop: number;
  paddingLeft: number;
  paddingRight: number;
  paddingBottom: number;
  isInExtract: boolean;
  isOutExtract: boolean;
  isFadedOut: boolean;
  displayName: string;

  constructor(node: Node) {
    this.node = node;
    this.expanded = false;
    this.x = 0;
    this.y = 0;
    this.width = 0;
    this.height = 0;
    this.inboxWidth = 0;
    this.outboxWidth = 0;

    this.excluded = false;

    // Params for bridge paths.
    this.structural = false;

    // Params for node box.
    this.labelOffset = 0;
    this.radius = 0;

    // Params for expanded node
    this.labelHeight = 0;
    this.paddingTop = 0;
    this.paddingLeft = 0;
    this.paddingRight = 0;
    this.paddingBottom = 0;
    this.isInExtract = false;
    this.isOutExtract = false;
    this.coreBox = { width: 0, height: 0 };

    // By default, we don't fade nodes out. Default to false for safety.
    this.isFadedOut = false;

    this.displayName = node.name.substring(
      node.name.lastIndexOf(NAMESPACE_DELIM) + 1);
  }

  isInCore(): boolean {
    return !this.isInExtract && !this.isOutExtract;
  }

}

export class RenderMetaedgeInfo {
  metaedge: Metaedge;
  adjoiningMetaedge: RenderMetaedgeInfo;
  structural: boolean;
  weight: number;
  points: Point[];
  edgeGroup: Selection<RenderMetaedgeInfo & any, any, any, any>;
  startMarkerId: string;
  endMarkerId: string;
  isFadedOut: boolean;

  constructor(metaedge: Metaedge) {
    this.metaedge = metaedge;
    this.adjoiningMetaedge = null;
    this.structural = false;
    this.weight = 1;
    this.isFadedOut = false;
  }

}

export class RenderGroupNodeInfo extends RenderNodeInfo {
  node: GroupNode;
  coreGraph: graphlib.Graph<RenderNodeInfo, RenderMetaedgeInfo>;
  inExtractBox: { width: number, height: number };
  outExtractBox: { width: number, height: number };
  /** Array of isolated in-extract nodes. */
  isolatedInExtract: RenderNodeInfo[];
  /** Array of isolated out-extract nodes. */
  isolatedOutExtract: RenderNodeInfo[];

  constructor(groupNode: GroupNode, graphOptions: GraphOptions) {
    super(groupNode);
    const metagraph = groupNode.metagraph;
    const gl = metagraph.graph();
    graphOptions.compound = true;
    this.coreGraph =
      createGraph<RenderNodeInfo, RenderMetaedgeInfo>(
        gl.name, GraphType.CORE, graphOptions);
    this.inExtractBox = { width: 0, height: 0 };
    this.outExtractBox = { width: 0, height: 0 };
    this.isolatedInExtract = [];
    this.isolatedOutExtract = [];

  }

}

export function buildRender(graphHierarchy: Hierarchy): Promise<RenderGraphInfo> {
  return runAsyncTask(() => {
    return new RenderGraphInfo(graphHierarchy, false);
  });
}
