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
import { Subject } from 'rxjs';
import { PARAMS as LAYOUT_PARAMS } from './layout';

export interface RelativePositionInfo {
  topLeft: { x: number, y: number };
  bottomRight: { x: number, y: number };
}


/**
 * 缩放控制器
 * zoomElement {SVGGElement} 被缩放的 svg 元素，必须被 containerEle 包含
 * containerEle {SVGSVGElement} 触发缩放的元素，一般为 zoomElement 的最外层容器
 */
export class DagreZoom {
  zoomTransform: d3.ZoomTransform;
  zoom: d3.ZoomBehavior<SVGElement, any>;
  transformChange: Subject<d3.ZoomTransform> = new Subject();

  constructor(public zoomElement: SVGGElement, public containerEle: SVGSVGElement) {
    this.bind();
  }

  bind() {
    this.zoom = d3.zoom<SVGElement, any>();
    this.zoom
    .on('end', () => {
      // TODO
    })
    .on('zoom', () => {
      this.zoomTransform = d3.event.transform;
      this.emitChange();
      d3.select(this.zoomElement).attr('transform', d3.event.transform);
    });
    d3.select(this.containerEle).call(this.zoom)
    .on('dblclick.zoom', null);
  }

  unbind() {
    if (this.zoom) {
      this.zoom
      .on('end', null)
      .on('zoom', null);
      this.zoom = null;
      this.transformChange.complete();
    }
  }

  /**
   * 缩放到合适的大小
   */
  fit(duration = 500) {
    const svgRect = this.containerEle.getBoundingClientRect();
    let sceneSize = null;
    try {
      sceneSize = this.zoomElement.getBBox();
      if (sceneSize.width === 0) {
        // There is no scene anymore. We have been detached from the dom.
        return;
      }
    } catch (e) {
      // Firefox produced NS_ERROR_FAILURE if we have been
      // detached from the dom.
      return;
    }


    const scale = 0.9 * Math.min(svgRect.width / sceneSize.width, svgRect.height / sceneSize.height, 2);

    const dx = (svgRect.width - sceneSize.width * scale) / 2;
    const dy = (svgRect.height - sceneSize.height * scale) / 2;
    const params = LAYOUT_PARAMS.graph;

    const transform = d3.zoomIdentity
    .translate(dx + params.padding.paddingLeft, dy + params.padding.paddingTop)
    .scale(scale);

    d3.select(this.containerEle)
    .transition()
    .duration(duration)
    .call(this.zoom.transform, transform)
    .on('end.fitted', () => {
      // Remove the listener for the zoomend event,
      // so we don't get called at the end of regular zoom events,
      // just those that fit the graph to screen.
      this.zoom.on('end.fitted', null);
    });
  }

  panToCenter(node: SVGGElement) {

    // 确保 node 在 这个 SVG 容器中
    if (!node || !this.containerEle.contains(node)) {
      return false;
    }

    const svgRect = this.containerEle.getBoundingClientRect();
    const position = this.getRelativePositionInfo(node);
    const svgTransform = d3.zoomTransform(this.containerEle);

    const centerX = (position.topLeft.x + position.bottomRight.x) / 2;
    const centerY = (position.topLeft.y + position.bottomRight.y) / 2;
    const dx = svgRect.left + svgRect.width / 2 - centerX;
    const dy = svgRect.top + svgRect.height / 2 - centerY;


    d3.select(this.containerEle).transition().duration(500).call(
      this.zoom.translateBy,
      dx / svgTransform.k,
      dy / svgTransform.k);

  }

  /**
   * 判断 node 位置是否在这个 SVG 容器中，
   * X 或者 Y 任意一边超出这返回 true，否则返回 false，
   * 如果 node 不存在，或不在这个 SVG 容器 DOM 中也返回 false
   */
  isOffScreen(node: SVGGElement) {

    // 确保 node 在 这个 SVG 容器中
    if (!node || !this.containerEle.contains(node)) {
      return false;
    }
    const position = this.getRelativePositionInfo(node);
    const svgRect = this.containerEle.getBoundingClientRect();
    const horizontalBound = svgRect.left + svgRect.width;
    const verticalBound = svgRect.top + svgRect.height;
    const isOutsideOfBounds = (start, end, lowerBound, upperBound) => {
      // Return if even a part of the interval is out of bounds.
      return !(start > lowerBound && end < upperBound);
    };

    // X 或者 Y 任意一边超出
    return isOutsideOfBounds(position.topLeft.x, position.bottomRight.x, svgRect.left, horizontalBound) ||
      isOutsideOfBounds(position.topLeft.y, position.bottomRight.y, svgRect.top, verticalBound);
  }

  private emitChange() {
    this.transformChange.next(this.zoomTransform);
  }

  /**
   * 获取 node 位于 SVG 容器的相对位置信息
   */
  private getRelativePositionInfo(node: SVGGElement): RelativePositionInfo {
    const nodeBox = node.getBBox();
    const nodeCtm = node.getScreenCTM();
    let pointTL = this.containerEle.createSVGPoint();
    let pointBR = this.containerEle.createSVGPoint();

    pointTL.x = nodeBox.x;
    pointTL.y = nodeBox.y;
    pointBR.x = nodeBox.x + nodeBox.width;
    pointBR.y = nodeBox.y + nodeBox.height;
    pointTL = pointTL.matrixTransform(nodeCtm);
    pointBR = pointBR.matrixTransform(nodeCtm);
    return {
      topLeft    : pointTL,
      bottomRight: pointBR
    };
  }
}
