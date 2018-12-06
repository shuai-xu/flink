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

import * as dagre from 'dagre';
import { runAsyncTask } from './util';
import { Hierarchy } from './hierarchy';
import {
  AttrDef,
  BaseEdge,
  GraphDef,
  NodeDef,
  NodeInputDef,
  Node,
  InclusionType,
  NodeType,
  Metanode,
  GroupNode,
  GraphType, NAMESPACE_DELIM, Metaedge
} from './interface';
import Graph = dagre.graphlib.Graph;

export const EDGE_KEY_DELIM = '--';


export class BaseNode implements NodeDef, Node {

  attr: AttrDef;
  inputs: NodeInputDef[];
  name: string;
  cardinality: number;
  include: InclusionType;
  isGroupNode: boolean;
  parentNode: Node;
  type: NodeType;
  constructor(rawNode: NodeDef) {
    this.name = rawNode.name;
    this.attr = rawNode.attr;
    this.inputs = rawNode.inputs;
    this.cardinality = 1;
    this.include = InclusionType.UNSPECIFIED;
    this.isGroupNode = false;
    this.parentNode = null;
    this.type = NodeType.OP;
  }
}

export class SlimGraph {
  nodes: { [nodeName: string]: BaseNode };
  edges: BaseEdge[];

  constructor() {
    this.nodes = {};
    this.edges = [];
  }
}

export function addEdgeToGraph(graph: SlimGraph, outputNode: BaseNode, input: NodeInputDef) {
  if (input.name === outputNode.name) {
    return;
  }

  graph.edges.push({
    ...input.attr,
    v: input.name,
    w: outputNode.name
  });
}

export function buildDef(graphDef: GraphDef): Promise<SlimGraph> {
  return runAsyncTask(() => {
    const graph = new SlimGraph();
    const graphNodes = graphDef.nodes.map(node => new BaseNode(node));
    graphNodes.forEach(node => {
      graph.nodes[node.name] = node;
      node.inputs.forEach(input => {
        addEdgeToGraph(graph, node, input);
      });
    });

    return graph;
  });
}

export class MetanodeImpl implements Metanode {
  associatedFunction: string;
  attr: AttrDef;
  bridgegraph: graphlib.Graph<GroupNode | BaseNode, Metaedge>;
  cardinality: number;
  compatibilityHistogram: { compatible: number; incompatible: number };
  depth: number;
  deviceHistogram: { [ p: string ]: number };
  hasNonControlEdges: boolean;
  include: InclusionType;
  isGroupNode: boolean;
  metagraph: graphlib.Graph<GroupNode | BaseNode, Metaedge>;
  name: string;
  opHistogram: { [ p: string ]: number };
  parentNode: Node;
  templateId: string;
  type: NodeType;
  xlaClusterHistogram: { [ p: string ]: number };


  constructor(name: string, opt = {}) {
    this.name = name;
    this.type = NodeType.META;

    this.depth = 1;
    this.isGroupNode = true;
    this.cardinality = 0;
    this.metagraph = createGraph<GroupNode | BaseNode, Metaedge>(name, GraphType.META, opt);
    this.bridgegraph = null;
    this.opHistogram = {};
    this.deviceHistogram = {};
    this.xlaClusterHistogram = {};
    this.compatibilityHistogram = {compatible: 0, incompatible: 0};
    /** unique id for a metanode of similar subgraph */
    this.templateId = null;
    /** Metanode which contains this node, if any */
    this.parentNode = null;
    this.hasNonControlEdges = false;
    this.include = InclusionType.UNSPECIFIED;
    this.associatedFunction = '';
  }
  getFirstChild(): GroupNode | BaseNode {
    return this.metagraph.node(this.metagraph.nodes()[0]);
  }

  getChildren(): Array<GroupNode | BaseNode> {
    return this.metagraph.nodes().map(node => this.metagraph.node(node));
  }

  getRootOp(): Node {
    return undefined;
  }

  leaves(): string[] {
    return [];
  }

}

export class MetaedgeImpl implements Metaedge {
  baseEdgeList: BaseEdge[];
  inbound: boolean;
  name: string;
  numControlEdges: number;
  numRefEdges: number;
  numRegularEdges: number;
  totalSize: number;
  v: string;
  w: string;

  constructor(v: string, w: string) {
    this.v = v;
    this.w = w;
    this.baseEdgeList = [];
    this.inbound = null;
    this.numRegularEdges = 0;
    this.numControlEdges = 0;
    this.numRefEdges = 0;
    this.totalSize = 0;
  }

  addBaseEdge(edge: BaseEdge, h: Hierarchy): void {
    this.baseEdgeList.push(edge);
  }

}

export function createMetanode(name: string, opt = {}): Metanode {
  return new MetanodeImpl(name, opt);
}

export function createMetaedge(v: string, w: string): Metaedge {
  return new MetaedgeImpl(v, w);
}

export function getHierarchicalPath(name: string): string[] {
  const path: string[] = [];
  let i = name.indexOf(NAMESPACE_DELIM);

  while (i >= 0) {
    path.push(name.substring(0, i));
    i = name.indexOf(NAMESPACE_DELIM, i + 1);
  }
  path.push(name);
  return path;
}


export function createGraph<N, E>(
  name: string,
  type,
  opt?: graphlib.GraphOptions): graphlib.Graph<N, E> {
  const graphOptions = opt || {};
  const graph = new Graph(graphOptions);
  graph.setGraph({
    name: name,
    rankdir: graphOptions.rankdir || 'BT',  // BT,TB,LR,RL
    type: type
  } as graphlib.GraphOptions);
  return (graph as any);
}
