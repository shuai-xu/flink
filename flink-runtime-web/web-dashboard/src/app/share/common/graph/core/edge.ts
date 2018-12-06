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

import * as d3 from 'd3';
import { select, Selection } from 'd3-selection';
import * as _ from 'lodash';
import { EDGE_KEY_DELIM } from './graph';
import { BaseEdge, Metaedge } from './interface';
/** The minimum stroke width of an edge. */
import { Point, RenderGraphInfo, RenderMetaedgeInfo, RenderNodeInfo } from './render';
import { Class, selectOrCreateChild, SVG_NAMESPACE } from './scene';

export const MIN_EDGE_WIDTH = 2.5;

/** The maximum stroke width of an edge. */
export const MAX_EDGE_WIDTH = 12;

/** Minimum stroke width to put edge labels in the middle of edges */
const CENTER_EDGE_LABEL_MIN_STROKE_WIDTH = 8;

export interface EdgeData {v: string; w: string; label: RenderMetaedgeInfo; }

export function getEdgeKey(edgeObj: EdgeData) {
  return edgeObj.v + EDGE_KEY_DELIM + edgeObj.w;
}

const arrowheadSizeArr: ReadonlyArray<string> = [ 'small', 'medium', 'large', 'xlarge' ];
const arrowheadMap =
      d3.scaleQuantize<String>().domain([MIN_EDGE_WIDTH, MAX_EDGE_WIDTH]).range(arrowheadSizeArr);

export let interpolate: d3.Line<{x: number, y: number}> = d3.line<{x: number, y: number}>()
.curve(d3.curveBasis)
.x((d) => d.x)
.y((d) => d.y);

export function buildGroupEdge(sceneGroup,
                           graph: graphlib.Graph<RenderNodeInfo, RenderMetaedgeInfo>,
                           sceneElement) {
  let edges: EdgeData[] = [];
  edges = _.reduce(graph.edges(), (_edges, edgeObj) => {
    const edgeLabel = graph.edge(edgeObj);
    _edges.push({
      v: edgeObj.v,
      w: edgeObj.w,
      label: edgeLabel
    });
    return _edges;
  }, edges);

  const container = selectOrCreateChild(sceneGroup, 'g', Class.Edge.CONTAINER);

  // Select all children and join with data.
  // (Note that all children of g.edges are g.edge)
  const edgeGroups = (container as any).selectAll(function() {return this.childNodes; }).data(edges, getEdgeKey);

  // Make edges a group to support rendering multiple lines for metaedge
  edgeGroups.enter()
  .append('g')
  .attr('class', Class.Edge.GROUP)
  .attr('data-edge', getEdgeKey)
  .each(function(d: EdgeData) {
    const edgeGroup = select(this);
    d.label.edgeGroup = edgeGroup;
    // index node group for quick highlighting
    sceneElement._edgeGroupIndex[getEdgeKey(d)] = edgeGroup;

    if (sceneElement.handleEdgeSelected) {
      // The user or some higher-level component has opted to make edges selectable.
      edgeGroup
      .on('click',
        _d => {
          // Stop this event's propagation so that it isn't also considered
          // a graph-select.
          (<Event>d3.event).stopPropagation();
          sceneElement.fire('edge-select', {
            edgeData: _d,
            edgeGroup: edgeGroup
          });
        });
    }

    // Add line during enter because we're assuming that type of line
    // normally does not change.
    appendEdge(edgeGroup, d, sceneElement);
  })
  .merge(edgeGroups)
  .each(position)
  .each(function(d) {
    stylize(select(this), d);
  });

  edgeGroups.exit()
  .each(d => {
    delete sceneElement._edgeGroupIndex[getEdgeKey(d)];
  })
  .remove();
  return edgeGroups;
}

export function appendEdge(edgeGroup, d: EdgeData,
                           sceneElement: {
                             renderHierarchy: RenderGraphInfo,
                             handleEdgeSelected: Function,
                           },
                           edgeClass?: string) {
  edgeClass = edgeClass || Class.Edge.LINE; // set default type

  if (d.label && d.label.structural) {
    edgeClass += ' ' + Class.Edge.STRUCTURAL;
  }
  if (d.label && d.label.metaedge && d.label.metaedge.numRefEdges) {
    edgeClass += ' ' + Class.Edge.REFERENCE_EDGE;
  }
  if (sceneElement.handleEdgeSelected) {
    // The user has opted to make edges selectable.
    edgeClass += ' ' + Class.Edge.SELECTABLE;
  }
  // Give the path a unique id, which will be used to link
  // the textPath (edge label) to this path.
  const pathId = 'path_' + getEdgeKey(d);

  let strokeWidth;
  if (sceneElement.renderHierarchy.edgeWidthFunction) {
    // Compute edge thickness based on the user-specified method.
    strokeWidth = sceneElement.renderHierarchy.edgeWidthFunction(d, edgeClass);
  } else {
    // Encode tensor size within edge thickness.
    let size = 1;
    if (d.label != null && d.label.metaedge != null) {
      // There is an underlying Metaedge.
      size = d.label.metaedge.totalSize;
    }
    strokeWidth = sceneElement.renderHierarchy.edgeWidthSizedBasedScale(size);
  }

  const path = edgeGroup.append('path')
  .attr('id', pathId)
  .attr('class', edgeClass)
  .style('stroke-width', strokeWidth + 'px');

  // Check if there is a reference edge and add an arrowhead of the right size.
  if (d.label && d.label.metaedge) {
    if (d.label.metaedge.numRefEdges) {
      // We have a reference edge.
      const markerId = `reference-arrowhead-${arrowheadMap(strokeWidth)}`;
      path.style('marker-start', `url(#${markerId})`);
      d.label.startMarkerId = markerId;
    } else {
      // We have a dataflow edge.
      const markerId = `dataflow-arrowhead-${arrowheadMap(strokeWidth)}`;
      path.style('marker-end', `url(#${markerId})`);
      d.label.endMarkerId = markerId;
    }
  }

  if (d.label == null || d.label.metaedge == null) {
    // There is no associated metaedge, thus no text.
    // This happens for annotation edges.
    return;
  }
  const labelForEdge = getLabelForEdge(d.label.metaedge,
    sceneElement.renderHierarchy);
  if (labelForEdge == null) {
    // We have no information to show on this edge.
    return;
  }

  // Put edge label in the middle of edge only if the edge is thick enough.
  const baseline = strokeWidth > CENTER_EDGE_LABEL_MIN_STROKE_WIDTH ?
    'central' :
    'text-after-edge';

  edgeGroup.append('text')
  .append('textPath')
  .attr('xlink:href', '#' + pathId)
  .attr('startOffset', '50%')
  .attr('text-anchor', 'middle')
  .attr('dominant-baseline', baseline)
  .text(labelForEdge);
}

function position(d) {
  d3.select(this)
  .select('path.' + Class.Edge.LINE)
  .transition()
  .attrTween('d', getEdgePathInterpolator as any);
}


/**
 * Creates the label for the given metaedge. If the metaedge consists
 * of only 1 tensor, and it's shape is known, the label will contain that
 * shape. Otherwise, the label will say the number of tensors in the metaedge.
 */
export function getLabelForEdge(metaedge: Metaedge,
                                renderInfo: RenderGraphInfo): string {
  if (renderInfo.edgeLabelFunction) {
    // The user has specified a means of computing the label.
    return renderInfo.edgeLabelFunction(metaedge, renderInfo);
  }
  // Compute the label based on either tensor count or size.
  const isMultiEdge = metaedge.baseEdgeList.length > 1;
  return isMultiEdge ?
    metaedge.baseEdgeList.length + ' operators' :
    getLabelForBaseEdge(metaedge.baseEdgeList[0], renderInfo);
}

/**
 * Returns the label for the given base edge.
 * The label is the shape of the underlying tensor.
 */
export function getLabelForBaseEdge(
  baseEdge: BaseEdge, renderInfo: RenderGraphInfo): string {
  return '';
}

/**
 * Returns a tween interpolator for the endpoint of an edge path.
 */
function getEdgePathInterpolator(d: EdgeData, i: number, a: string) {
  const renderMetaedgeInfo = <RenderMetaedgeInfo> d.label;
  const adjoiningMetaedge = renderMetaedgeInfo.adjoiningMetaedge;
  let points = renderMetaedgeInfo.points;
  // Adjust the path so that start/end markers point to the end
  // of the path.
  if (d.label.startMarkerId) {
    points = adjustPathPointsForMarker(
      points, d3.select('#' + d.label.startMarkerId), true);
  }
  if (d.label.endMarkerId) {
    points = adjustPathPointsForMarker(
      points, d3.select('#' + d.label.endMarkerId), false);
  }

  if (!adjoiningMetaedge) {
    return d3.interpolate(a, interpolate(points));
  }

  const renderPath = this;
  // Get the adjoining path that matches the adjoining metaedge.
  const adjoiningPath =
        <SVGPathElement>((<HTMLElement>adjoiningMetaedge.edgeGroup.node())
          .firstChild);

  // Find the desired SVGPoint along the adjoining path, then convert those
  // coordinates into the space of the renderPath using its Current
  // Transformation Matrix (CTM).
  const inbound = renderMetaedgeInfo.metaedge.inbound;

  return function(t) {
    const adjoiningPoint = adjoiningPath
    .getPointAtLength(inbound ? adjoiningPath.getTotalLength() : 0)
    .matrixTransform(adjoiningPath.getCTM())
    .matrixTransform(renderPath.getCTM().inverse());

    // Update the relevant point in the renderMetaedgeInfo's points list, then
    // re-interpolate the path.
    const index = inbound ? 0 : points.length - 1;
    points[index].x = adjoiningPoint.x;
    points[index].y = adjoiningPoint.y;
    return interpolate(points);
  };
}

/**
 * Shortens the path enought such that the tip of the start/end marker will
 * point to the start/end of the path. The marker can be of arbitrary size.
 *
 * @param points Array of path control points.
 * @param marker D3 selection of the <marker> svg element.
 * @param isStart Is the marker a `start-marker`. If false, the marker is
 *     an `end-marker`.
 * @return The new array of control points.
 */
function adjustPathPointsForMarker(points: Point[],
                                   marker: Selection<any, any, any, any>, isStart: boolean): Point[] {
  const lineFunc = d3.line<Point>()
  .x(d => d.x)
  .y(d => d.y);
  const path =
        d3.select(document.createElementNS('http://www.w3.org/2000/svg', 'path'))
        .attr('d', lineFunc(points));
  const markerWidth = +marker.attr('markerWidth');
  const viewBox = marker.attr('viewBox').split(' ').map(Number);
  const viewBoxWidth = viewBox[2] - viewBox[0];
  const refX = +marker.attr('refX');
  const pathNode = <SVGPathElement> path.node();
  if (isStart) {
    // The edge flows downwards. Do not make the edge go the whole way, lest we
    // clobber the arrowhead.
    const fractionStickingOut = 1 - refX / viewBoxWidth;
    const length = markerWidth * fractionStickingOut;
    const point = pathNode.getPointAtLength(length);
    // Figure out how many segments of the path we need to remove in order
    // to shorten the path.
    const segIndex = getPathSegmentIndexAtLength(points, length, lineFunc);
    // Update the very first segment.
    points[segIndex - 1] = {x: point.x, y: point.y};
    // Ignore every point before segIndex - 1.
    return points.slice(segIndex - 1);
  } else {
    // The edge flows upwards. Do not make the edge go the whole way, lest we
    // clobber the arrowhead.
    const fractionStickingOut = 1 - refX / viewBoxWidth;
    const length =
            pathNode.getTotalLength() - markerWidth * fractionStickingOut;
    const point = pathNode.getPointAtLength(length);
    // Figure out how many segments of the path we need to remove in order
    // to shorten the path.
    const segIndex = getPathSegmentIndexAtLength(points, length, lineFunc);
    // Update the very last segment.
    points[segIndex] = {x: point.x, y: point.y};
    // Ignore every point after segIndex.
    return points.slice(0, segIndex + 1);
  }
}

/**
 * Computes the index into a set of points that constitute a path for which the
 * distance along the path from the initial point is as large as possible
 * without exceeding the length. This function was introduced after the
 * native getPathSegAtLength method got deprecated by SVG 2.
 * @param points Array of path control points. A point has x and y properties.
 *   Must be of length at least 2.
 * @param length The length (float).
 * @param lineFunc A function that takes points and returns the "d" attribute
 *   of a path made from connecting the points.
 * @return The index into the points array.
 */
function getPathSegmentIndexAtLength(
  points: Point[],
  length: number,
  lineFunc: (points: Point[]) => string): number {
  const path: SVGPathElement = document.createElementNS(SVG_NAMESPACE, 'path');
  for (let i = 1; i < points.length; i++) {
    path.setAttribute('d', lineFunc(points.slice(0, i)));
    if (path.getTotalLength() > length) {
      // This many points has already exceeded the length.
      return i - 1;
    }
  }
  // The entire path is shorter than the specified length.
  return points.length - 1;
}


/**
 * For a given d3 selection and data object, mark the edge as a control
 * dependency if it contains only control edges.
 *
 * d's label property will be a RenderMetaedgeInfo object.
 */
function stylize(edgeGroup, d: EdgeData) {
  edgeGroup.classed('faded', d.label.isFadedOut);
  // const metaedge = d.label.metaedge;
  // edgeGroup.select('path.' + Class.Edge.LINE)
  // .classed('control-dep', metaedge && !metaedge.numRegularEdges);
}
