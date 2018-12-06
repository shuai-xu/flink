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

import { BaseNode } from './graph';
import { Hierarchy } from './hierarchy';

export const ROOT_NAME = '__root__';
export const BRIDGE_GRAPH_NAME = '__bridgegraph__';
export const NAMESPACE_DELIM = '/';

export enum NodeType {META, OP, SERIES, BRIDGE, ELLIPSIS}

export enum InclusionType {INCLUDE, EXCLUDE, UNSPECIFIED}

export enum GraphType {FULL, EMBEDDED, META, SERIES, CORE, SHADOW, BRIDGE, EDGE}

export interface GraphDef {
  nodes: NodeDef[];
}

export interface NodeDef {
  name: string;
  inputs: NodeInputDef[];
  attr: AttrDef;
}

export interface NodeInputDef {
  name: string;
  attr: AttrDef;
}

export interface AttrDef {
  [ key: string ]: any;
}

export interface BaseEdge {
  w: string;
  v: string;
  name?: string;
}

export interface HierarchyParams {
  rankDirection: 'TB' | 'BT' | 'LR' | 'RL';
}

export interface BridgeNode extends Node {
  /**
   * Whether this bridge node represents edges coming into its parent node.
   */
  inbound: boolean;
}

export interface Node {
  name: string;
  type: NodeType;
  isGroupNode: boolean;
  cardinality: number;
  parentNode: Node;
  include: InclusionType;
  attr: AttrDef;
}

export interface Edges {
  control: Metaedge[];
  regular: Metaedge[];
}


export interface Metanode extends GroupNode {
  depth: number;
  templateId: string;
  opHistogram: { [ op: string ]: number };

  associatedFunction: string;

  getFirstChild(): GroupNode | Node;
  getChildren(): Array<GroupNode | BaseNode>;
  getRootOp(): Node;

  leaves(): string[];
}

export interface Metaedge extends graphlib.EdgeObject {

  /**
   * Stores the original BaseEdges represented by this Metaedge.
   */
  baseEdgeList: BaseEdge[];

  /**
   * Whether this edge represents a relationship that is inbound (or outbound)
   * to the object which contains this information. For example, in a Metanode's
   * bridgegraph, each edge connects an immediate child to something outside
   * the Metanode. If the destination of the edge is inside the Metanode, then
   * its inbound property should be true. If the destination is outside the
   * Metanode, then its inbound property should be false.
   *
   * The property is optional because not all edges can be described as
   * inbound/outbound. For example, in a Metanode's metagraph, all of the edges
   * connect immediate children of the Metanode. None should have an inbound
   * property, or they should be null/undefined.
   */
  inbound?: boolean;

  /**
   * Number of regular edges (not control dependency edges).
   */
  numRegularEdges: number;

  /**
   * Number of control dependency edges.
   */
  numControlEdges: number;

  /**
   * Number of reference edges, which is an edge to an operation
   * that takes a reference to its input and changes its value.
   */
  numRefEdges: number;

  /**
   * Total size (number of units) of all the tensors flowing through this edge.
   */
  totalSize: number;

  addBaseEdge(edge: BaseEdge, h: Hierarchy): void;
}

export interface GroupNode extends Node {
  metagraph: graphlib.Graph<GroupNode | BaseNode, Metaedge>;
  bridgegraph: graphlib.Graph<GroupNode | BaseNode, Metaedge>;
  deviceHistogram: { [ device: string ]: number };
  xlaClusterHistogram: { [ device: string ]: number };
  compatibilityHistogram: { compatible: number, incompatible: number };
  hasNonControlEdges: boolean;
}

