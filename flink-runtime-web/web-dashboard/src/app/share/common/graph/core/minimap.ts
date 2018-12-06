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

const FRAC_VIEWPOINT_AREA = 0.8;

export class Minimap {

  private minimap: HTMLElement;
  private canvas: HTMLCanvasElement;
  private canvasRect: ClientRect;
  private canvasBuffer: HTMLCanvasElement;
  private download: HTMLLinkElement;
  private downloadCanvas: HTMLCanvasElement;
  private minimapSvg: SVGSVGElement;
  private viewpoint: SVGRectElement;
  private scaleMinimap: number;
  private scaleMain: number;
  private maxWandH: number;
  private translate: [number, number];
  private viewpointCoord: {x: number, y: number};
  private minimapSize: {width: number, height: number};
  private labelPadding: number;

  private svg: SVGSVGElement;
  private zoomG: SVGGElement;
  private mainZoom: d3.ZoomBehavior<any, any>;

  constructor(
    svg: SVGSVGElement,
    zoomG: SVGGElement,
    mainZoom: d3.ZoomBehavior<any, any>,
    minimap: HTMLElement,
    maxWandH: number,
    labelPadding: number
  ) {
    this.svg = svg;
    this.labelPadding = labelPadding;
    this.zoomG = zoomG;
    this.mainZoom = mainZoom;
    this.maxWandH = maxWandH;
    const $minimap = d3.select(minimap);
    const $minimapSvg = $minimap.select('svg');
    const $viewpoint = $minimapSvg.select('rect');
    this.canvas = <HTMLCanvasElement>$minimap.select('canvas.first').node();
    this.canvasRect = this.canvas.getBoundingClientRect();
    const dragmove = (d) => {
      this.viewpointCoord.x = (<DragEvent>d3.event).x;
      this.viewpointCoord.y = (<DragEvent>d3.event).y;
      this.updateViewpoint();
    };
    this.viewpointCoord = {x: 0, y: 0};
    const drag = d3.drag().subject(Object).on('drag', dragmove);
    $viewpoint.datum(this.viewpointCoord as any).call(drag);

    // Make the minimap clickable.
    $minimapSvg.on('click', () => {
      if ((<Event>d3.event).defaultPrevented) {
        // This click was part of a drag event, so suppress it.
        return;
      }
      // Update the coordinates of the viewpoint.
      const minimapOffset = this.minimapOffset();
      const width = Number($viewpoint.attr('width'));
      const height = Number($viewpoint.attr('height'));
      const clickCoords = d3.mouse($minimapSvg.node() as any);
      this.viewpointCoord.x = clickCoords[0] - width / 2 - minimapOffset.x;
      this.viewpointCoord.y = clickCoords[1] - height / 2 - minimapOffset.y;
      this.updateViewpoint();
    });
    this.viewpoint = <SVGRectElement>$viewpoint.node();
    this.minimapSvg = <SVGSVGElement>$minimapSvg.node();
    this.minimap = minimap;
    this.canvasBuffer =
      <HTMLCanvasElement>$minimap.select('canvas.second').node();
    this.downloadCanvas =
      <HTMLCanvasElement>$minimap.select('canvas.download').node();
    d3.select(this.downloadCanvas).style('display', 'none');
    this.update();
  }

  private minimapOffset() {
    return {
      x: (this.canvasRect.width - this.minimapSize.width) / 2,
      y: (this.canvasRect.height - this.minimapSize.height) / 2,
    };
  }

  private updateViewpoint(): void {
    // Update the coordinates of the viewpoint rectangle.
    d3.select(this.viewpoint)
    .attr('x', this.viewpointCoord.x)
    .attr('y', this.viewpointCoord.y);
    // Update the translation vector of the main svg to reflect the
    // new viewpoint.
    const mainX = - this.viewpointCoord.x * this.scaleMain / this.scaleMinimap;
    const mainY = - this.viewpointCoord.y * this.scaleMain / this.scaleMinimap;
    d3.select(this.svg).call(
      this.mainZoom.transform,
      d3.zoomIdentity.translate(mainX, mainY).scale(this.scaleMain));
  }

  update(): void {
    let sceneSize = null;
    try {
      // Get the size of the entire scene.
      sceneSize = this.zoomG.getBBox();
      if (sceneSize.width === 0) {
        // There is no scene anymore. We have been detached from the dom.
        return;
      }
    } catch (e) {
      // Firefox produced NS_ERROR_FAILURE if we have been
      // detached from the dom.
      return;
    }
    const $download = d3.select('#graphdownload');
    this.download = <HTMLLinkElement>$download.node();
    $download.on('click', d => {
      this.download.href = this.downloadCanvas.toDataURL('image/png');
    });

    const $svg = d3.select(this.svg);
    // Read all the style rules in the document and embed them into the svg.
    // The svg needs to be self contained, i.e. all the style rules need to be
    // embedded so the canvas output matches the origin.
    let stylesText = '';
    for (let k = 0; k < document.styleSheets.length; k++) {
      try {
        const cssRules = (<any>document.styleSheets[k]).cssRules ||
          (<any>document.styleSheets[k]).rules;
        if (cssRules == null) {
          continue;
        }
        for (let i = 0; i < cssRules.length; i++) {
          // Remove tf-* selectors from the styles.
          stylesText +=
            cssRules[i].cssText.replace(/ ?tf-[\w-]+ ?/g, '') + '\n';
        }
      } catch (e) {
        if (e.name !== 'SecurityError') {
          throw e;
        }
      }
    }

    // Temporarily add the css rules to the main svg.
    const svgStyle = $svg.append('style');
    svgStyle.text(stylesText);

    // Temporarily remove the zoom/pan transform from the main svg since we
    // want the minimap to show a zoomed-out and centered view.
    const $zoomG = d3.select(this.zoomG);
    const zoomTransform = $zoomG.attr('transform');
    $zoomG.attr('transform', null);

    // Since we add padding, account for that here.
    sceneSize.height += this.labelPadding * 2;
    sceneSize.width += this.labelPadding * 2;

    // Temporarily assign an explicit width/height to the main svg, since
    // it doesn't have one (uses flex-box), but we need it for the canvas
    // to work.
    $svg
    .attr('width', sceneSize.width)
    .attr('height', sceneSize.height);

    // Since the content inside the svg changed (e.g. a node was expanded),
    // the aspect ratio have also changed. Thus, we need to update the scale
    // factor of the minimap. The scale factor is determined such that both
    // the width and height of the minimap are <= maximum specified w/h.
    this.scaleMinimap =
      this.maxWandH / Math.max(sceneSize.width, sceneSize.height);

    this.minimapSize = {
      width: sceneSize.width * this.scaleMinimap,
      height: sceneSize.height * this.scaleMinimap
    };

    const minimapOffset = this.minimapOffset();

    // Update the size of the minimap's svg, the buffer canvas and the
    // viewpoint rect.
    d3.select(this.minimapSvg).attr(<any>this.minimapSize);
    d3.select(this.canvasBuffer).attr(<any>this.minimapSize);

    // Download canvas width and height are multiples of the style width and
    // height in order to increase pixel density of the PNG for clarity.
    const downloadCanvasSelection = d3.select(this.downloadCanvas);
    downloadCanvasSelection.style('width', sceneSize.width);
    downloadCanvasSelection.style('height', sceneSize.height);
    downloadCanvasSelection.attr('width', 3 * sceneSize.width);
    downloadCanvasSelection.attr('height', 3 * sceneSize.height);

    if (this.translate != null && this.zoom != null) {
      // Update the viewpoint rectangle shape since the aspect ratio of the
      // map has changed.
      requestAnimationFrame(() => this.zoom());
    }

    // Serialize the main svg to a string which will be used as the rendering
    // content for the canvas.
    const svgXml = (new XMLSerializer()).serializeToString(this.svg);

    // Now that the svg is serialized for rendering, remove the temporarily
    // assigned styles, explicit width and height and bring back the pan/zoom
    // transform.
    svgStyle.remove();
    $svg.attr('width', null).attr('height', null);

    $zoomG.attr('transform', zoomTransform);
    const image = new Image();
    image.onload = () => {
      // Draw the svg content onto the buffer canvas.
      const context = this.canvasBuffer.getContext('2d');
      context.clearRect(0, 0, this.canvasBuffer.width,
        this.canvasBuffer.height);

      context.drawImage(image, minimapOffset.x, minimapOffset.y,
        this.minimapSize.width, this.minimapSize.height);
      requestAnimationFrame(() => {
        // Hide the old canvas and show the new buffer canvas.
        d3.select(this.canvasBuffer).style('display', null);
        d3.select(this.canvas).style('display', 'none');
        // Swap the two canvases.
        [this.canvas, this.canvasBuffer] = [this.canvasBuffer, this.canvas];
      });
      const downloadContext = this.downloadCanvas.getContext('2d');
      downloadContext.clearRect(0, 0, this.downloadCanvas.width,
        this.downloadCanvas.height);
      downloadContext.drawImage(image, 0, 0,
        this.downloadCanvas.width, this.downloadCanvas.height);
    };
    image.onerror = () => {
      const blob = new Blob([svgXml], {type: 'image/svg+xml;charset=utf-8'});
      image.src = URL.createObjectURL(blob);
    };
    image.src =
      'data:image/svg+xml;charset=utf-8,' + encodeURIComponent(svgXml);
  }

  /**
   * Handles changes in zooming/panning. Should be called from the main svg
   * to notify that a zoom/pan was performed and this minimap will update it's
   * viewpoint rectangle.
   *
   * @param translate The translate vector, or none to use the last used one.
   * @param scale The scaling factor, or none to use the last used one.
   */
  zoom(transform?: d3.ZoomTransform): void {
    if (this.scaleMinimap == null) {
      // Scene is not ready yet.
      return;
    }
    // Update the new translate and scale params, only if specified.
    if (transform) {
      this.translate = [transform.x, transform.y];
      this.scaleMain = transform.k;
    }

    // Update the location of the viewpoint rectangle.
    const svgRect = this.svg.getBoundingClientRect();
    const minimapOffset = this.minimapOffset();
    const $viewpoint = d3.select(this.viewpoint);
    this.viewpointCoord.x = -this.translate[0] * this.scaleMinimap /
      this.scaleMain;
    this.viewpointCoord.y = -this.translate[1] * this.scaleMinimap /
      this.scaleMain;
    const viewpointWidth = svgRect.width * this.scaleMinimap / this.scaleMain;
    const viewpointHeight = svgRect.height * this.scaleMinimap / this.scaleMain;
    $viewpoint
    .attr('x', this.viewpointCoord.x + minimapOffset.x)
    .attr('y', this.viewpointCoord.y + minimapOffset.y)
    .attr('width', viewpointWidth)
    .attr('height', viewpointHeight);
    // Show/hide the minimap depending on the viewpoint area as fraction of the
    // whole minimap.
    const mapWidth = this.minimapSize.width;
    const mapHeight = this.minimapSize.height;
    const x = this.viewpointCoord.x;
    const y = this.viewpointCoord.y;
    const w = Math.min(Math.max(0, x + viewpointWidth), mapWidth) -
      Math.min(Math.max(0, x), mapWidth);
    const h = Math.min(Math.max(0, y + viewpointHeight), mapHeight) -
      Math.min(Math.max(0, y), mapHeight);
    const fracIntersect = (w * h) / (mapWidth * mapHeight);
    if (fracIntersect < FRAC_VIEWPOINT_AREA) {
      this.minimap.classList.remove('hidden');
    } else {
      this.minimap.classList.add('hidden');
    }
  }
}
