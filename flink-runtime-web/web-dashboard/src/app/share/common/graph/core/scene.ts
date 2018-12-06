/**
 * ==============================================================================
 * This product contains a modified version of 'TensorBoard plugin for
 * graphs', a Angular implementation of nest-graph visualization
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

import * as _ from 'lodash';
import { buildGroupEdge } from './edge';
import { NodeType } from './interface';
import { buildGroupNode } from './node';
import { RenderGroupNodeInfo, RenderNodeInfo } from './render';
import { select, Selection } from 'd3-selection';
import { computeCXPositionOfNodeShape, MIN_AUX_WIDTH, PARAMS as LAYOUT_PARAMS } from './layout';
import 'd3-transition';

export const SVG_NAMESPACE = 'http://www.w3.org/2000/svg';


export let Class = {
  Node        : {
    // <g> element that contains nodes.
    CONTAINER       : 'nodes',
    // <g> element that contains detail about a node.
    GROUP           : 'node',
    // <g> element that contains visual elements (like rect, ellipse).
    SHAPE           : 'nodeshape',
    // <*> element(s) under SHAPE that should receive color updates.
    COLOR_TARGET    : 'nodecolortarget',
    // <text> element showing the node's label.
    LABEL           : 'nodelabel',
    SUB_TITLE       : 'node-groupSubtitle',
    // <g> element that contains all visuals for the expand/collapse
    // button for expandable group nodes.
    BUTTON_CONTAINER: 'buttoncontainer',
    // <circle> element that surrounds expand/collapse buttons.
    BUTTON_CIRCLE   : 'buttoncircle',
    // <path> element of the expand button.
    EXPAND_BUTTON   : 'expandbutton',
    // <path> element of the collapse button.
    COLLAPSE_BUTTON : 'collapsebutton'
  },
  Edge        : {
    CONTAINER     : 'edges',
    GROUP         : 'edge',
    LINE          : 'edgeline',
    REFERENCE_EDGE: 'referenceedge',
    REF_LINE      : 'refline',
    SELECTABLE    : 'selectableedge',
    SELECTED      : 'selectededge',
    STRUCTURAL    : 'structural'
  },
  Annotation  : {
    OUTBOX      : 'out-annotations',
    INBOX       : 'in-annotations',
    GROUP       : 'annotation',
    NODE        : 'annotation-node',
    EDGE        : 'annotation-edge',
    CONTROL_EDGE: 'annotation-control-edge',
    LABEL       : 'annotation-label',
    ELLIPSIS    : 'annotation-ellipsis'
  },
  Scene       : {
    GROUP           : 'scene',
    CORE            : 'core',
    FUNCTION_LIBRARY: 'function-library',
    INEXTRACT       : 'in-extract',
    OUTEXTRACT      : 'out-extract'
  },
  Subscene    : { GROUP: 'subscene' },
  OPNODE      : 'op',
  METANODE    : 'meta',
  SERIESNODE  : 'series',
  BRIDGENODE  : 'bridge',
  ELLIPSISNODE: 'ellipsis'
};

export function buildGroupScene(container: Selection<SVGGElement, any, any, any>,
                                renderNode: RenderGroupNodeInfo,
                                sceneElement?,
                                sceneClass = Class.Scene.GROUP): Selection<any, any, any, any> {
  const isNewSceneGroup = selectChild(container, 'g', sceneClass).empty();
  const sceneGroup = selectOrCreateChild(container, 'g', sceneClass);
  // groupCore
  const coreGroup = selectOrCreateChild(sceneGroup, 'g', Class.Scene.CORE);
  const coreNodes = _.reduce(renderNode.coreGraph.nodes(), (nodes, name) => {
    const node = renderNode.coreGraph.node(name);
    if (!node.excluded) {
      nodes.push(node);
    }
    return nodes;
  }, []);

  if (renderNode.node.type === NodeType.SERIES) {
    // For series, we want the first item on top, so reverse the array so
    // the first item in the series becomes last item in the top, and thus
    // is rendered on the top.
    coreNodes.reverse();
  }

  // requestAnimationFrame 避免多节点时掉帧

  // Create the layer of edges for this scene (paths).
  requestAnimationFrame(() => {
    buildGroupEdge(coreGroup, renderNode.coreGraph, sceneElement);
  });

  // Create the layer of nodes for this scene (ellipses, rects etc).
  requestAnimationFrame(() => {
    buildGroupNode(coreGroup, coreNodes, sceneElement);
  });

  // In-extract
  selectChild(sceneGroup, 'g', Class.Scene.INEXTRACT).remove();
  // Out-extract
  selectChild(sceneGroup, 'g', Class.Scene.OUTEXTRACT).remove();

  selectChild(sceneGroup, 'g', Class.Scene.FUNCTION_LIBRARY).remove();


  position(sceneGroup, renderNode);

  // Fade in the scene group if it didn't already exist.
  if (isNewSceneGroup) {
    sceneGroup.attr('opacity', 0).transition().attr('opacity', 1);
  }

  return sceneGroup;
}

export function selectChild(
  container, tagName: string, className?: string | string[]): Selection<any, any, any, any> {
  const children = container.node().childNodes;
  for (let i = 0; i < children.length; i++) {
    const child = children[ i ];
    if (child.tagName === tagName) {
      if (className instanceof Array) {
        let hasAllClasses = true;
        for (let j = 0; j < className.length; j++) {
          hasAllClasses =
            hasAllClasses && child.classList.contains(className[ j ]);
        }
        if (hasAllClasses) {
          return select(child);
        }
      } else if ((!className || child.classList.contains(className))) {
        return select(child);
      }
    }
  }
  return select(null);
}


export function selectOrCreateChild(
  container, tagName: string, className?: string | string[], before?): Selection<any, any, any, any> {
  const child = selectChild(container, tagName, className);
  if (!child.empty()) {
    return child;
  }
  const newElement =
          document.createElementNS('http://www.w3.org/2000/svg', tagName);

  if (className instanceof Array) {
    for (let i = 0; i < className.length; i++) {
      newElement.classList.add(className[ i ]);
    }
  } else {
    newElement.classList.add(className);
  }

  if (before) { // if before exists, insert
    container.node().insertBefore(newElement, before);
  } else { // otherwise, append
    container.node().appendChild(newElement);
  }
  return select(newElement)
  // need to bind data to emulate d3_selection.append
  .datum(container.datum());
}

function position(sceneGroup, renderNode: RenderGroupNodeInfo) {
  // Translate scenes down by the label height so that when showing graphs in
  // expanded metanodes, the graphs are below the labels.  Do not shift them
  // down for series nodes as series nodes don't have labels inside of their
  // bounding boxes.
  const yTranslate = renderNode.node.type === NodeType.SERIES ?
    0 : LAYOUT_PARAMS.subscene.meta.labelHeight;

  // groupCore
  translate(selectChild(sceneGroup, 'g', Class.Scene.CORE), 0, yTranslate);

  // in-extract
  const hasInExtract = renderNode.isolatedInExtract.length > 0;
  const hasOutExtract = renderNode.isolatedOutExtract.length > 0;

  const offset = LAYOUT_PARAMS.subscene.meta.extractXOffset;

  let auxWidth = 0;
  if (hasInExtract) {
    auxWidth += renderNode.outExtractBox.width;
  }
  if (hasOutExtract) {
    auxWidth += renderNode.outExtractBox.width;
  }

  if (hasInExtract) {
    let inExtractX = renderNode.coreBox.width;
    if (auxWidth < MIN_AUX_WIDTH) {
      inExtractX = inExtractX - MIN_AUX_WIDTH +
        renderNode.inExtractBox.width / 2;
    } else {
      inExtractX = inExtractX -
        renderNode.inExtractBox.width / 2 - renderNode.outExtractBox.width -
        (hasOutExtract ? offset : 0);
    }
    translate(
      selectChild(sceneGroup, 'g', Class.Scene.INEXTRACT), inExtractX,
      yTranslate);
  }

  // out-extract
  if (hasOutExtract) {
    let outExtractX = renderNode.coreBox.width;
    if (auxWidth < MIN_AUX_WIDTH) {
      outExtractX = outExtractX - MIN_AUX_WIDTH +
        renderNode.outExtractBox.width / 2;
    } else {
      outExtractX -= renderNode.outExtractBox.width / 2;
    }

    translate(
      selectChild(sceneGroup, 'g', Class.Scene.OUTEXTRACT), outExtractX,
      yTranslate);
  }
}

export function translate(selection, x0: number, y0: number) {
  // If it is already placed on the screen, make it a transition.
  if (selection.attr('transform') != null) {
    selection = selection.transition('position');
  }
  selection.attr('transform', 'translate(' + x0 + ',' + y0 + ')');
}

/**
 * Helper for setting position of a svg expand/collapse button
 * @param button container group
 * @param renderNode the render node of the group node to position
 *        the button on.
 */
export function positionButton(button, renderNode: RenderNodeInfo) {
  const cx = computeCXPositionOfNodeShape(renderNode);
  // Position the button in the top-right corner of the group node,
  // with space given the draw the button inside of the corner.
  const width = renderNode.expanded ?
    renderNode.width : renderNode.coreBox.width;
  const height = renderNode.expanded ?
    renderNode.height : renderNode.coreBox.height;
  let x = cx + width / 2 - 6;
  let y = renderNode.y - height / 2 + 6;

  // For unexpanded series nodes, the button has special placement due
  // to the unique visuals of this group node.
  if (renderNode.node.type === NodeType.SERIES && !renderNode.expanded) {
    x += 10;
    y -= 2;
  }
  const translateStr = 'translate(' + x + ',' + y + ')';
  button.selectAll('path').transition().attr('transform', translateStr);
  button.select('circle').transition().attr(
    { cx: x, cy: y, r: LAYOUT_PARAMS.nodeSize.meta.expandButtonRadius });
}

/**
 * Helper for setting position of a svg ellipse
 * @param ellipse ellipse to set position of.
 * @param cx Center x.
 * @param cy Center x.
 * @param width Width to set.
 * @param height Height to set.
 */
export function positionEllipse(ellipse, cx: number, cy: number,
                                width: number, height: number) {
  ellipse.transition()
  .attr('cx', cx)
  .attr('cy', cy)
  .attr('rx', width / 2)
  .attr('ry', height / 2);
}

/**
 * Helper for setting position of a svg rect
 * @param rect A d3 selection of rect(s) to set position of.
 * @param cx Center x.
 * @param cy Center x.
 * @param width Width to set.
 * @param height Height to set.
 * @param scale scale
 */
export function positionRect(rect, cx: number, cy: number, width: number,
                             height: number, scale = 1) {
  rect.transition()
  .attr('style', `transform: scale(${1 / scale})`)
  .attr('x', (cx - width / 2) * scale)
  .attr('y', (cy - height / 2) * scale)
  .attr('width', width * scale)
  .attr('height', height * scale);
}


export function getNodeElementByName(nodeName: string) {
  return select('[data-name="' + nodeName + '"].' + Class.Node.GROUP).node();
}
