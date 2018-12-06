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

import { scaleLinear } from 'd3-scale';
import { layout } from 'dagre';
import * as _ from 'lodash';
import { NodeType } from './interface';

/** Set of parameters that define the look and feel of the graph. */
import { Point, RenderGroupNodeInfo, RenderMetaedgeInfo, RenderNodeInfo } from './render';

export const PARAMS = {
  animation   : {
    /** Default duration for graph animations in ms. */
    duration: 250
  },
  graph       : {
    /** Graph parameter for metanode. */
    meta   : {
      /**
       * Dagre's nodesep param - number of pixels that
       * separate nodes horizontally in the layout.
       *
       * See https://github.com/cpettitt/dagre/wiki#configuring-the-layout
       */
      nodeSep: 50,
      /**
       * Dagre's ranksep param - number of pixels
       * between each rank in the layout.
       *
       * See https://github.com/cpettitt/dagre/wiki#configuring-the-layout
       */
      rankSep: 40,
      /**
       * Dagre's edgesep param - number of pixels that separate
       * edges horizontally in the layout.
       */
      edgeSep: 5
    },
    /** Graph parameter for metanode. */
    series : {
      /**
       * Dagre's nodesep param - number of pixels that
       * separate nodes horizontally in the layout.
       *
       * See https://github.com/cpettitt/dagre/wiki#configuring-the-layout
       */
      nodeSep: 5,
      /**
       * Dagre's ranksep param - number of pixels
       * between each rank in the layout.
       *
       * See https://github.com/cpettitt/dagre/wiki#configuring-the-layout
       */
      rankSep: 25,
      /**
       * Dagre's edgesep param - number of pixels that separate
       * edges horizontally in the layout.
       */
      edgeSep: 5
    },
    /**
     * Padding is used to correctly position the graph SVG inside of its parent
     * element. The padding amounts are applied using an SVG transform of X and
     * Y coordinates.
     */
    padding: { paddingTop: 10, paddingLeft: 0 }
  },
  subscene    : {
    meta  : {
      paddingTop    : 20,
      paddingBottom : 20,
      paddingLeft   : 20,
      paddingRight  : 20,
      /**
       * Used to leave room for the label on top of the highest node in
       * the groupCore graph.
       */
      labelHeight   : 20,
      /** X-space between each extracted node and the groupCore graph. */
      extractXOffset: 0,
      /** Y-space between each extracted node. */
      extractYOffset: 0
    },
    series: {
      paddingTop   : 10,
      paddingBottom: 10,
      paddingLeft  : 10,
      paddingRight : 10,
      labelHeight  : 10
    }
  },
  nodeSize    : {
    /** Size of meta nodes. */
    meta  : {
      radius            : 2,
      width             : 230,
      maxLabelWidth     : 0,
      /** A scale for the node's height based on number of nodes inside */
      // Hack - set this as an any type to avoid issues in exporting a type
      // from an external module.
      height            : scaleLinear().domain([ 1, 200 ]).range([ 15, 60 ] as ReadonlyArray<number>).clamp(true),
      /** The radius of the circle denoting the expand button. */
      expandButtonRadius: 3
    },
    /** Size of op nodes. */
    op    : {
      width        : 230,
      height       : scaleLinear().domain([ 1, 2, 3 ]).range([ 85, 100, 115 ]  as ReadonlyArray<number>),
      radius       : 1,  // for making annotation touching ellipse
      labelOffset  : 10,
      maxLabelWidth: 40
    },
    /** Size of series nodes. */
    series: {
      expanded  : {
        // For expanded series nodes, width and height will be
        // computed to account for the subscene.
        radius     : 10,
        labelOffset: 0
      },
      vertical  : {
        // When unexpanded, series whose underlying metagraphs contain
        // one or more non-control edges will show as a vertical stack
        // of ellipses.
        width      : 16,
        height     : 13,
        labelOffset: -13
      },
      horizontal: {
        // When unexpanded, series whose underlying metagraphs contain
        // no non-control edges will show as a horizontal stack of
        // ellipses.
        width      : 24,
        height     : 8,
        radius     : 10,  // Forces annotations to center line.
        labelOffset: -10
      }
    },
    /** Size of bridge nodes. */
    bridge: {
      // NOTE: bridge nodes will normally be invisible, but they must
      // take up some space so that the layout step leaves room for
      // their edges.
      width      : 20,
      height     : 20,
      radius     : 2,
      labelOffset: 0
    }
  },
  shortcutSize: {
    /** Size of shortcuts for op nodes */
    op    : { width: 10, height: 4 },
    /** Size of shortcuts for meta nodes */
    meta  : { width: 12, height: 4, radius: 1 },
    /** Size of shortcuts for series nodes */
    series: {
      width : 14,
      height: 4
    }
  },
  annotations : {
    /** Maximum possible width of the bounding box for in annotations */
    inboxWidth   : 50,
    /** Maximum possible width of the bounding box for out annotations */
    outboxWidth  : 50,
    /** X-space between the shape and each annotation-node. */
    xOffset      : 10,
    /** Y-space between each annotation-node. */
    yOffset      : 3,
    /** X-space between each annotation-node and its label. */
    labelOffset  : 2,
    /** Defines the max width for annotation label */
    maxLabelWidth: 120
  },
  constant    : { size: { width: 4, height: 4 } },
  series      : {
    /** Maximum number of repeated item for unexpanded series node. */
    maxStackCount           : 3,
    /**
     * Positioning offset ratio for collapsed stack
     * of parallel series (series without edges between its members).
     */
    parallelStackOffsetRatio: 0.2,
    /**
     * Positioning offset ratio for collapsed stack
     * of tower series (series with edges between its members).
     */
    towerStackOffsetRatio   : 0.5
  },
  minimap     : {
    /** The maximum width/height the minimap can have. */
    size: 150
  }
};

/**
 * The minimum width we confer upon the auxiliary nodes section if functions
 * also appear. Without enforcing this minimum, metanodes in the function
 * library section could jut into the auxiliary nodes section because the
 * title "Auxiliary Nodes" is longer than the width of the auxiliary nodes
 * section itself.
 */
export const MIN_AUX_WIDTH = 0;

/** Calculate layout for a scene of a group node. */
export function layoutScene(renderNodeInfo: RenderGroupNodeInfo): void {
  // Update layout, size, and annotations of its children nodes and edges.
  if (renderNodeInfo.node.isGroupNode) {
    layoutChildren(renderNodeInfo);
  }

  // Update position of its children nodes and edges
  if (renderNodeInfo.node.type === NodeType.META) {
    layoutMetanode(renderNodeInfo);
  } else if (renderNodeInfo.node.type === NodeType.SERIES) {
    layoutSeriesNode(renderNodeInfo);
  }
}

/**
 * 更新其子节点与 edges 的布局和大小
 */
export function layoutChildren(renderNodeInfo: RenderGroupNodeInfo): void {

  const children = renderNodeInfo.coreGraph.nodes().map(n => {
    return renderNodeInfo.coreGraph.node(n);
  }).concat(renderNodeInfo.isolatedInExtract,
    renderNodeInfo.isolatedOutExtract);

  children.forEach(childNodeInfo => {
    // Set size of each child
    switch (childNodeInfo.node.type) {
      case NodeType.OP:
        Object.assign(childNodeInfo, PARAMS.nodeSize.op);
        const nameLength = childNodeInfo.node.attr[ 'name' ].length;
        childNodeInfo.height = PARAMS.nodeSize.op.height(Math.ceil((nameLength + 2) / 28));

        break;
      case NodeType.BRIDGE:
        Object.assign(childNodeInfo, PARAMS.nodeSize.bridge);
        break;
      case NodeType.META:
        if (!childNodeInfo.expanded) {
          // Set fixed width and scalable height based on cardinality
          Object.assign(childNodeInfo, PARAMS.nodeSize.meta);
          childNodeInfo.height = 165;
          // PARAMS.nodeSize.meta.height(childNodeInfo.node.cardinality);
        } else {
          const childGroupNodeInfo =
                  <RenderGroupNodeInfo>childNodeInfo;
          layoutScene(childGroupNodeInfo); // Recursively layout its subscene.
        }
        break;
      case NodeType.SERIES:
        if (childNodeInfo.expanded) {
          Object.assign(childNodeInfo, PARAMS.nodeSize.series.expanded);
          const childGroupNodeInfo =
                  <RenderGroupNodeInfo>childNodeInfo;
          layoutScene(childGroupNodeInfo); // Recursively layout its subscene.
        } else {
          const childGroupNodeInfo =
                  <RenderGroupNodeInfo>childNodeInfo;
          const seriesParams =
                  childGroupNodeInfo.node.hasNonControlEdges ?
                    PARAMS.nodeSize.series.vertical :
                    PARAMS.nodeSize.series.horizontal;
          Object.assign(childNodeInfo, seriesParams);
        }
        break;
      default:
        throw Error('Unrecognized node type: ' + childNodeInfo.node.type);
    }
    // Compute total width of un-expanded nodes. Width of expanded nodes
    // has already been computed.
    if (!childNodeInfo.expanded) {
      updateTotalWidthOfNode(childNodeInfo);
    }
  });
}

export function layoutMetanode(renderNodeInfo: RenderGroupNodeInfo): void {
  // First, copy params specific to meta nodes onto this render info object.
  const params = PARAMS.subscene.meta;
  Object.assign(renderNodeInfo, params);
  // Invoke dagre.layout() on the groupCore graph and record the bounding box
  // dimensions.
  Object.assign(renderNodeInfo.coreBox,
    dagreLayout(renderNodeInfo.coreGraph, PARAMS.graph.meta));

  // Calculate the position of nodes in isolatedInExtract relative to the
  // top-left corner of inExtractBox (the bounding box for all inExtract nodes)
  // and calculate the size of the inExtractBox.
  const maxInExtractWidth = renderNodeInfo.isolatedInExtract.length ?
    _.maxBy(
      renderNodeInfo.isolatedInExtract,
      renderNode => renderNode.width
    ).width : null;
  renderNodeInfo.inExtractBox.width = maxInExtractWidth != null ?
    maxInExtractWidth : 0;

  renderNodeInfo.inExtractBox.height =
    _.reduce(renderNodeInfo.isolatedInExtract, (height, child, i) => {
      const yOffset = i > 0 ? params.extractYOffset : 0;
      // use width/height here to avoid overlaps between extracts
      child.x = 0;
      child.y = height + yOffset + child.height / 2;
      return height + yOffset + child.height;
    }, 0);

  // Calculate the position of nodes in isolatedOutExtract relative to the
  // top-left corner of outExtractBox (the bounding box for all outExtract
  // nodes) and calculate the size of the outExtractBox.
  const maxOutExtractWidth = renderNodeInfo.isolatedOutExtract.length ?
    _.maxBy(
      renderNodeInfo.isolatedOutExtract,
      renderNode => renderNode.width
    ).width : null;
  renderNodeInfo.outExtractBox.width = maxOutExtractWidth != null ?
    maxOutExtractWidth : 0;

  renderNodeInfo.outExtractBox.height =
    _.reduce(renderNodeInfo.isolatedOutExtract, (height, child, i) => {
      const yOffset = i > 0 ? params.extractYOffset : 0;
      // use width/height here to avoid overlaps between extracts
      child.x = 0;
      child.y = height + yOffset + child.height / 2;
      return height + yOffset + child.height;
    }, 0);

  // Calculate the position of nodes in libraryFunctionsExtract relative to the
  // top-left corner of libraryFunctionsBox (the bounding box for all library
  // function nodes) and calculate the size of the libraryFunctionsBox.


  // Compute the total padding between the groupCore graph, in-extract and
  // out-extract boxes.
  let numParts = 0;
  if (renderNodeInfo.isolatedInExtract.length > 0) {
    numParts++;
  }
  if (renderNodeInfo.isolatedOutExtract.length > 0) {
    numParts++;
  }

  if (renderNodeInfo.coreGraph.nodeCount() > 0) {
    numParts++;
  }
  const offset = PARAMS.subscene.meta.extractXOffset;
  const padding = numParts <= 1 ? 0 : (numParts * offset);

  // Add the in-extract and out-extract width to the groupCore box width. Do not let
  // the auxiliary width be too small, lest it be smaller than the title.
  const auxWidth = Math.max(
    MIN_AUX_WIDTH,
    renderNodeInfo.inExtractBox.width + renderNodeInfo.outExtractBox.width);

  renderNodeInfo.coreBox.width += auxWidth + padding + padding;
  renderNodeInfo.coreBox.height =
    params.labelHeight +
    Math.max(
      renderNodeInfo.inExtractBox.height,
      renderNodeInfo.coreBox.height,
      renderNodeInfo.outExtractBox.height
    );
  // Determine the whole metanode's width (from left to right).

  renderNodeInfo.width = renderNodeInfo.coreBox.width +
    params.paddingLeft + params.paddingRight;

  // Determine the whole metanode's height (from top to bottom).
  renderNodeInfo.height =
    renderNodeInfo.paddingTop +
    renderNodeInfo.coreBox.height +
    renderNodeInfo.paddingBottom;
}

export function layoutSeriesNode(node: RenderGroupNodeInfo): void {
  const graph = node.coreGraph;

  const params = PARAMS.subscene.series;
  Object.assign(node, params);

  // Layout the groupCore.
  Object.assign(node.coreBox, dagreLayout(node.coreGraph, PARAMS.graph.series));

  graph.nodes().forEach(nodeName => {
    graph.node(nodeName).excluded = false;
  });

  // Series do not have in/outExtractBox so no need to include them here.
  node.width = node.coreBox.width + params.paddingLeft + params.paddingRight;
  node.height = node.coreBox.height + params.paddingTop + params.paddingBottom;
}

function updateTotalWidthOfNode(renderInfo: RenderNodeInfo): void {
  // Assign the width of the groupCore box (the main shape of the node).
  renderInfo.coreBox.width = renderInfo.width;
  renderInfo.coreBox.height = renderInfo.height;
  // TODO: Account for font width rather than using a magic number.
  const labelLength = renderInfo.displayName.length;
  const charWidth = 3; // 3 pixels per character.
  // Compute the total width of the node.
  renderInfo.width = Math.max(renderInfo.coreBox.width +
    renderInfo.inboxWidth + renderInfo.outboxWidth,
    labelLength * charWidth);
}

/**
 * Calculate layout for a graph using dagre
 * @param graph the graph to be laid out
 * @param params layout parameters
 * @return width and height of the groupCore graph
 */
function dagreLayout(
  graph: graphlib.Graph<RenderNodeInfo, RenderMetaedgeInfo>,
  params): { height: number, width: number } {
  Object.assign(graph.graph(), {
    nodesep: params.nodeSep,
    ranksep: params.rankSep,
    edgesep: params.edgeSep
  });
  const bridgeNodeNames = [];
  const nonBridgeNodeNames = [];

  // Split out nodes into bridge and non-bridge nodes, and calculate the total
  // width we should use for bridge nodes.
  graph.nodes().forEach(nodeName => {
    const nodeInfo = graph.node(nodeName);
    if (nodeInfo.node.type === NodeType.BRIDGE) {
      bridgeNodeNames.push(nodeName);
    } else {
      nonBridgeNodeNames.push(nodeName);
    }
  });

  graph.edges().forEach(e => {
    const edge = graph.edge(e) as any;
    if (!edge.structural) {
      const maxLabelLength = Math.max(edge.metaedge.baseEdgeList.map(_e => (_e.partitioner || '').length));
      const rankdir = graph.graph().rankdir;
      const rankSep = edge.metaedge.inbound ? graph.graph().ranksep : Math.max(params.rankSep, maxLabelLength * 5);
      if (['RL', 'LR'].indexOf(rankdir) !== -1) {
        edge.width = rankSep;
      } else {
        edge.height = rankSep;
      }
    }
  });

  // If there are no non-bridge nodes, then the graph has zero size.
  if (!nonBridgeNodeNames.length) {
    return {
      width : 0,
      height: 0
    };
  }
  layout(graph as any);

  // Calculate the true bounding box of the graph by iterating over nodes and
  // edges rather than accepting dagre's word for it. In particular, we should
  // ignore the extra-wide bridge nodes and bridge edges, and allow for
  // annotation boxes and labels.
  let minX = Infinity;
  let minY = Infinity;
  let maxX = -Infinity;
  let maxY = -Infinity;
  nonBridgeNodeNames.forEach(nodeName => {
    const nodeInfo = graph.node(nodeName);
    const w = 0.5 * nodeInfo.width;
    const x1 = nodeInfo.x - w;
    const x2 = nodeInfo.x + w;
    minX = x1 < minX ? x1 : minX;
    maxX = x2 > maxX ? x2 : maxX;
    // TODO: Account for the height of labels above op nodes here.
    const h = 0.5 * nodeInfo.height;
    const y1 = nodeInfo.y - h;
    const y2 = nodeInfo.y + h;
    minY = y1 < minY ? y1 : minY;
    maxY = y2 > maxY ? y2 : maxY;
  });

  graph.edges().forEach(edgeObj => {
    const edgeInfo = graph.edge(edgeObj);
    if (edgeInfo.structural) {
      return; // Skip structural edges from min/max calculations.
    }

    // Since the node size passed to dagre includes the in and out
    // annotations, the endpoints of the edge produced by dagre may not
    // point to the actual node shape (rectangle, ellipse). We correct the
    // end-points by finding the intersection of a line between the
    // next-to-last (next-to-first) point and the destination (source)
    // rectangle.
    const sourceNode = graph.node(edgeInfo.metaedge.v);
    const destNode = graph.node(edgeInfo.metaedge.w);

    // Straight 3-points edges are special case, since they are curved after
    // our default correction. To keep them straight, we remove the mid point
    // and correct the first and the last point to be the center of the
    // source and destination node respectively.
    if (edgeInfo.points.length === 3 && isStraightLine(edgeInfo.points)) {
      if (sourceNode != null) {
        const cxSource = sourceNode.expanded ?
          sourceNode.x : computeCXPositionOfNodeShape(sourceNode);
        edgeInfo.points[ 0 ].x = cxSource;
      }
      if (destNode != null) {
        const cxDest = destNode.expanded ?
          destNode.x : computeCXPositionOfNodeShape(destNode);
        edgeInfo.points[ 2 ].x = cxDest;
      }
      // Remove the middle point so the edge doesn't curve.
      edgeInfo.points = [ edgeInfo.points[ 0 ], edgeInfo.points[ 1 ] ];
    }
    // Correct the destination endpoint of the edge.
    const nextToLastPoint = edgeInfo.points[ edgeInfo.points.length - 2 ];
    // The destination node might be null if this is a bridge edge.
    if (destNode != null) {
      edgeInfo.points[ edgeInfo.points.length - 1 ] =
        intersectPointAndNode(nextToLastPoint, destNode);
    }
    // Correct the source endpoint of the edge.
    const secondPoint = edgeInfo.points[ 1 ];
    // The source might be null if this is a bridge edge.
    if (sourceNode != null) {
      edgeInfo.points[ 0 ] = intersectPointAndNode(secondPoint, sourceNode);
    }

    edgeInfo.points.forEach((point: Point) => {
      minX = point.x < minX ? point.x : minX;
      maxX = point.x > maxX ? point.x : maxX;
      minY = point.y < minY ? point.y : minY;
      maxY = point.y > maxY ? point.y : maxY;
    });
  });

  // Shift all nodes and edge points to account for the left-padding amount,
  // and the invisible bridge nodes.
  graph.nodes().forEach(nodeName => {
    const nodeInfo = graph.node(nodeName);
    nodeInfo.x -= minX;
    nodeInfo.y -= minY;
  });

  graph.edges().forEach(edgeObj => {
    graph.edge(edgeObj).points.forEach((point: Point) => {
      point.x -= minX;
      point.y -= minY;
    });
  });

  return {
    width : maxX - minX,
    height: maxY - minY
  };
}

/**
 * Returns if a line going through the specified points is a straight line.
 */
function isStraightLine(points: Point[]) {
  let angle = angleBetweenTwoPoints(points[ 0 ], points[ 1 ]);
  for (let i = 1; i < points.length - 1; i++) {
    const newAngle = angleBetweenTwoPoints(points[ i ], points[ i + 1 ]);
    // Have a tolerance of 1 degree.
    if (Math.abs(newAngle - angle) > 1) {
      return false;
    }
    angle = newAngle;
  }
  return true;
}


/** Returns the angle (in degrees) between two points. */
function angleBetweenTwoPoints(a: Point, b: Point): number {
  const dx = b.x - a.x;
  const dy = b.y - a.y;
  return 180 * Math.atan(dy / dx) / Math.PI;
}

/**
 * Determines the center position of the node's shape. The position depends
 * on if the node has in and out-annotations.
 */
export function computeCXPositionOfNodeShape(renderInfo: RenderNodeInfo):
  number {
  if (renderInfo.expanded) {
    return renderInfo.x;
  }
  const dx = 0;
  return renderInfo.x - renderInfo.width / 2 + dx +
    renderInfo.coreBox.width / 2;
}

/**
 * Returns the intersection of a line between the provided point
 * and the provided rectangle.
 */
function intersectPointAndNode(
  point: Point, node: RenderNodeInfo): Point {
  // cx and cy are the center of the rectangle.
  const cx = node.expanded ?
    node.x : computeCXPositionOfNodeShape(node);
  const cy = node.y;
  // Calculate the slope
  const dx = point.x - cx;
  const dy = point.y - cy;
  let w = node.expanded ? node.width : node.coreBox.width;
  let h = node.expanded ? node.height : node.coreBox.height;
  let deltaX, deltaY;
  if (Math.abs(dy) * w / 2 > Math.abs(dx) * h / 2) {
    // The intersection is above or below the rectangle.
    if (dy < 0) {
      h = -h;
    }
    deltaX = dy === 0 ? 0 : h / 2 * dx / dy;
    deltaY = h / 2;
  } else {
    // The intersection is left or right of the rectangle.
    if (dx < 0) {
      w = -w;
    }
    deltaX = w / 2;
    deltaY = dx === 0 ? 0 : w / 2 * dy / dx;
  }
  return { x: cx + deltaX, y: cy + deltaY };
}
