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

import { Component, ElementRef, OnInit, Inject, forwardRef, AfterContentInit } from '@angular/core';
import { PARAMS as LAYOUT_PARAMS } from '../core/layout';
import { Minimap } from '../core/minimap';
import { NzGraphComponent } from '../graph/graph.component';

@Component({
  selector: 'flink-graph-minmap',
  templateUrl: './graph-minmap.component.html',
  styleUrls: ['./graph-minmap.component.less']
})
export class NzGraphMinmapComponent implements OnInit, AfterContentInit {

  hostElement: HTMLElement;
  minimap: Minimap;
  constructor(
    private hostRef: ElementRef<HTMLElement>,
    @Inject(forwardRef(() => NzGraphComponent)) private graphComponent: NzGraphComponent,
  ) {
    if (!this.graphComponent) {
      throw new TypeError('nz-graph');
    }

  }

  ngOnInit() {
    this.hostElement = this.hostRef.nativeElement;
    this.init();
  }

  ngAfterContentInit(): void {

  }

  init() {
    this.minimap = new Minimap(
      this.graphComponent.$svg.nativeElement,
      this.graphComponent.$root.nativeElement,
      this.graphComponent.zoom.zoom,
      this.hostElement,
      LAYOUT_PARAMS.minimap.size,
      LAYOUT_PARAMS.subscene.meta.labelHeight);
  }

  zoom(transform) {
    if (this.minimap) {
      this.minimap.zoom(transform);
    }
  }

  update() {
    if (this.minimap) {
      this.minimap.update();
    }
  }
}
