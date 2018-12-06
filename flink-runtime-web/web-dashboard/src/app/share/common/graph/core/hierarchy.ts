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

import { runAsyncTask } from './util';
import { BaseNode, createGraph, createMetaedge, createMetanode, getHierarchicalPath, SlimGraph } from './graph';
import {
  BRIDGE_GRAPH_NAME, Edges,
  GraphType,
  GroupNode,
  HierarchyParams, Metaedge,
  Metanode,
  Node,
  ROOT_NAME
} from './interface';

export class Hierarchy {

  private index: { [ nodeName: string ]: GroupNode | BaseNode };

  root: Metanode;
  maxMetaEdgeSize = 1;
  hasShapeInfo = false;

  graphOptions: graphlib.GraphOptions;
  templates: { [ templateId: string ]: string[] };
  devices: string[];
  xlaClusters: string[];
  orderings: { [ nodeName: string ]: { [ childName: string ]: number } };

  constructor(graphOptions: graphlib.GraphOptions) {
    this.graphOptions = graphOptions || {};
    this.graphOptions.compound = true;
    this.root = createMetanode(ROOT_NAME, this.graphOptions);
    this.templates = null;
    this.devices = null;
    this.xlaClusters = null;

    this.index = {};
    this.index[ ROOT_NAME ] = this.root;
    this.orderings = {};

  }

  getNodeMap(): { [ nodeName: string ]: GroupNode | BaseNode } {
    return this.index;
  }

  node(name: string): GroupNode | BaseNode {
    return this.index[ name ];
  }

  setNode(name: string, node: GroupNode | BaseNode): void {
    this.index[ name ] = node;
  }

  getBridgegraph(nodeName: string): graphlib.Graph<GroupNode | BaseNode, Metaedge> {
    const node = this.index[ nodeName ];
    if (!node) {
      throw Error('Could not find node in hierarchy: ' + nodeName);
    }

    if (!('metagraph' in node)) {
      return null;
    }

    const groupNode = node as GroupNode;
    if (groupNode.bridgegraph) {
      return groupNode.bridgegraph;
    }

    const bridgegraph = createGraph<GroupNode | BaseNode, Metaedge>(BRIDGE_GRAPH_NAME, GraphType.BRIDGE, this.graphOptions);
    groupNode.bridgegraph = bridgegraph;

    if (!node.parentNode || !('metagraph' in node.parentNode)) {
      return bridgegraph;
    }

    const parentNode = node.parentNode as GroupNode;
    const parentMetagraph = parentNode.metagraph;
    const parentBridgegraph = this.getBridgegraph(parentNode.name);

    [ parentMetagraph, parentBridgegraph ].forEach(parentGraph => {
      parentGraph
      .edges()
      .filter(e => e.v === nodeName || e.w === nodeName)
      .forEach(parentEdgeObj => {
        const inbound = parentEdgeObj.w === nodeName;
        const parentMetaedge = parentGraph.edge(parentEdgeObj);

        parentMetaedge.baseEdgeList.forEach(baseEdge => {
          const [ descendantName, otherName ] =
                inbound ?
                  [ baseEdge.w, parentEdgeObj.v ] :
                  [ baseEdge.v, parentEdgeObj.w ];
          const childName = this.getChildName(nodeName, descendantName);

          const bridgeEdgeObj = <graphlib.EdgeObject> {
            v: inbound ? otherName : childName,
            w: inbound ? childName : otherName,
          };
          let bridgeMetaedge = bridgegraph.edge(bridgeEdgeObj);

          if (!bridgeMetaedge) {
            bridgeMetaedge = createMetaedge(bridgeEdgeObj.v, bridgeEdgeObj.w);
            bridgeMetaedge.inbound = inbound;
            bridgegraph.setEdge(bridgeEdgeObj.v, bridgeEdgeObj.w,
              bridgeMetaedge);
          }
          bridgeMetaedge.addBaseEdge(baseEdge, this);

        });
      });
    });
    return bridgegraph;
  }

  getChildName(nodeName: string, descendantName: string): string {
    // Walk up the hierarchy from the descendant to find the child.
    let currentNode: Node = this.index[descendantName];
    while (currentNode) {
      if (currentNode.parentNode && currentNode.parentNode.name === nodeName) {
        return currentNode.name;
      }
      currentNode = currentNode.parentNode;
    }
    throw Error(
      'Could not find immediate child for descendant: ' + descendantName);
  }

  getPredecessors(nodeName: string): Edges {
    const node = this.index[nodeName];
    if (!node) {
      throw Error('Could not find node with name: ' + nodeName);
    }
    return this.getOneWayEdges(node, true);
  }

  getSuccessors(nodeName: string): Edges {
    const node = this.index[nodeName];
    if (!node) {
      throw Error('Could not find node with name: ' + nodeName);
    }
    return this.getOneWayEdges(node, false);

  }

  getOneWayEdges(node: GroupNode|BaseNode, inEdges: boolean) {
    const edges: Edges = {control: [], regular: []};
    // A node with no parent cannot have any edges.
    if (!node.parentNode || !node.parentNode.isGroupNode) {
      return edges;
    }
    const parentNode = <GroupNode> node.parentNode;
    const metagraph = parentNode.metagraph;
    const bridgegraph = this.getBridgegraph(parentNode.name);
    findEdgeTargetsInGraph(metagraph, node, inEdges, edges);
    findEdgeTargetsInGraph(bridgegraph, node, inEdges, edges);
    return edges;
  }

}

export function buildHierarchy(graph: SlimGraph, params: HierarchyParams) {
  const h = new Hierarchy({ 'rankdir': params.rankDirection });
  return runAsyncTask(() => addNodes(h, graph))
  // groupSeries
  .then(() => addEdges(h, graph))
  .then(() => h);
}

function addNodes(h: Hierarchy, graph: SlimGraph) {

  Object.keys(graph.nodes).forEach(key => {
    const node = graph.nodes[ key ];
    const path = getHierarchicalPath(node.name);
    let parent: Metanode = h.root;
    parent.depth = Math.max(path.length, parent.depth);

    for (let i = 0; i < path.length; i++) {
      parent.depth = Math.max(parent.depth, path.length - i);
      parent.cardinality += node.cardinality;
      if (i === path.length - 1) {
        break;
      }
      const name = path[ i ];
      let child = <Metanode>h.node(name);
      if (!child) {
        child = createMetanode(name, h.graphOptions);
        child.parentNode = parent;
        h.setNode(name, child);
        parent.metagraph.setNode(name, child);
      }
      parent = child;
    }

    h.setNode(node.name, node);
    node.parentNode = parent;
    parent.metagraph.setNode(node.name, node);
  });
}

function addEdges(h: Hierarchy, graph: SlimGraph) {
  const nodeIndex = h.getNodeMap();

  const sourcePath: string[] = [];
  const destPath: string[] = [];

  const getPath = (node: Node, path: string[]): number => {
    let i = 0;
    while (node) {
      path[ i++ ] = node.name;
      node = node.parentNode;
    }
    return i - 1;
  };

  graph.edges.forEach(baseEdge => {
    let sourceAncestorIndex = getPath(graph.nodes[ baseEdge.v ], sourcePath);
    let destAncestorIndex = getPath(graph.nodes[ baseEdge.w ], destPath);

    if (sourceAncestorIndex === -1 || destAncestorIndex === -1) {
      return;
    }

    // Find the lowest shared ancestor between source and dest by looking for
    // the highest nodes that differ between their ancestor paths.
    while (sourcePath[ sourceAncestorIndex ] === destPath[ destAncestorIndex ]) {
      sourceAncestorIndex--;
      destAncestorIndex--;
      if (sourceAncestorIndex < 0 || destAncestorIndex < 0) {
        // This would only occur if the two nodes were the same (a cycle in the
        // graph), or if one endpoint was a strict ancestor of the other. The
        // latter shouldn't happen because we rename nodes which are both
        // metanodes and op nodes. E.g. 'A/B' becomes 'A/B/(B)'.
        throw Error('No difference found between ancestor paths.');
      }
    }

    const sharedAncestorNode =
            <GroupNode>nodeIndex[ sourcePath[ sourceAncestorIndex + 1 ] ];
    const sourceAncestorName = sourcePath[ sourceAncestorIndex ];
    const destAncestorName = destPath[ destAncestorIndex ];

    // Find or create the Metaedge which should contain this BaseEdge inside
    // the shared ancestor.
    let metaedge =
          sharedAncestorNode.metagraph.edge(sourceAncestorName, destAncestorName);
    if (!metaedge) {
      metaedge = createMetaedge(sourceAncestorName, destAncestorName);
      sharedAncestorNode.metagraph
      .setEdge(sourceAncestorName, destAncestorName, metaedge);
    }
    (metaedge as Metaedge).addBaseEdge(baseEdge, h);

  });
}

/**
 * Internal utility function - given a graph (should be either a metagraph or a
 * bridgegraph) and a node which is known to be in that graph, determine
 * the other ends of edges that involve that node in the direction specified
 * by whether it's inbound.
 *
 * For example if you wanted to find the predecessors of a node, you'd call
 * this method for the parent's metagraph and bridgegraph, specifying inbound
 * as true (look at the source of inbound edges to the specified node).
 *
 * Discovered target names are appended to the targets array.
 */
function findEdgeTargetsInGraph(
  graph: graphlib.Graph<GroupNode|BaseNode, Metaedge>,
  node: Node, inbound: boolean, targets: Edges): void {
  const edges = inbound ? graph.inEdges(node.name) : graph.outEdges(node.name);
  edges.forEach(e => {
    const metaedge = graph.edge(e);
    const targetList =
            metaedge.numRegularEdges ? targets.regular : targets.control;
    targetList.push(metaedge);
  });
}
