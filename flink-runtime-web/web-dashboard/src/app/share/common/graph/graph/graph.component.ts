/**
 * ==============================================================================
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

import { ComponentPortal, ComponentType, DomPortalHost, PortalInjector } from '@angular/cdk/portal';
import {
  AfterContentInit,
  ApplicationRef,
  ChangeDetectionStrategy,
  Component,
  ComponentFactoryResolver,
  ElementRef,
  EventEmitter, InjectionToken,
  Injector, Input,
  OnDestroy,
  OnInit,
  Output,
  TemplateRef,
  ViewChild,
  ViewContainerRef
} from '@angular/core';

import * as d3 from 'd3';
import * as _ from 'lodash';
import { Subject } from 'rxjs';

import { buildDef, SlimGraph } from '../core/graph';
import { buildHierarchy, Hierarchy } from '../core/hierarchy';
import { GraphDef, Node, NodeType, ROOT_NAME } from '../core/interface';
import { layoutScene } from '../core/layout';
import { stylize, traceInputs } from '../core/node';
import { buildRender, RenderGraphInfo, RenderGroupNodeInfo, RenderNodeInfo } from '../core/render';
import { buildGroupScene, getNodeElementByName } from '../core/scene';
import { DagreZoom } from '../core/zoom';
import { NzGraphMinmapComponent } from '../graph-minmap/graph-minmap.component';

export const RENDER_NODE_INFO = new InjectionToken<{}>('RENDER_NODE_INFO');
export const RENDER_NODE_INFO_CHANGE = new InjectionToken<{}>('RENDER_NODE_INFO_CHANGE');

@Component({
  selector       : 'flink-graph',
  templateUrl    : './graph.component.html',
  styleUrls      : [ './graph.component.less' ],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class NzGraphComponent implements OnInit, AfterContentInit, OnDestroy {

  _edgeGroupIndex = [];
  _nodeGroupIndex = {};
  maxMetanodeLabelLength = 20;
  maxMetanodeLabelLengthLargeFont = 8;
  maxMetanodeLabelLengthFontSize = 8;
  minMetanodeLabelLengthFontSize = 8;

  selectedNode: string;

  portalCacheMap: Map<RenderGroupNodeInfo | RenderNodeInfo, { host: DomPortalHost, temp: ComponentPortal<any> }> = new Map();
  @ViewChild('root') $root: ElementRef<SVGGElement>;
  @ViewChild('svg') $svg: ElementRef<SVGSVGElement>;
  @ViewChild(NzGraphMinmapComponent) minmap: NzGraphMinmapComponent;
  @ViewChild('nodeTmp') nodeTmp: TemplateRef<{ $implicit: RenderGroupNodeInfo }>;
  @Output() zoomInit = new EventEmitter<void>();
  @Output() zoomChange: EventEmitter<d3.ZoomTransform> = new EventEmitter<d3.ZoomTransform>();
  @Output() event: EventEmitter<{eventName: string, event: any}> =  new EventEmitter();

  @Input() nodePortal: ComponentType<any>;
  zoom: DagreZoom;
  graph: SlimGraph;
  graphHierarchy: Hierarchy;
  renderHierarchy: RenderGraphInfo;
  sceneGroup: d3.Selection<SVGGElement, any, any, any>;

  constructor(
    private componentFactoryResolver: ComponentFactoryResolver,
    private viewContainerRef: ViewContainerRef,
    private injector: Injector,
    private appRef: ApplicationRef
  ) {
  }

  /**
   * 接收一个 GraphDef 对象，返回 Hierarchy 类
   */
  buildGraph(graphDef: GraphDef): Promise<Hierarchy> {
    // 构建 GraphDef
    return buildDef(graphDef)
    .then(graph => {
      this.graph = graph;
      // 构建 Hierarchy
      return buildHierarchy(graph, { rankDirection: 'LR' });
    })
    .then(graphHierarchy => {
      this.graphHierarchy = graphHierarchy;
      return this.graphHierarchy;
    });
  }

  /**
   * 接收一个 Hierarchy 对象，然后返回一个拥有坐标信息的 RenderGraphInfo 对象
   */
  buildRenderGraphInfo(graphHierarchy: Hierarchy): Promise<RenderGraphInfo> {
    return buildRender(graphHierarchy)
    .then(renderGraph => {
      this.renderHierarchy = renderGraph;
      return this.renderHierarchy;
    });
  }

  layoutScene(renderHierarchy: RenderGroupNodeInfo) {
    layoutScene(renderHierarchy);
  }

  buildGroupScene(renderHierarchy: RenderGroupNodeInfo) {
    this.sceneGroup = buildGroupScene(d3.select(this.$root.nativeElement), renderHierarchy, this);
  }

  nodeToggleExpand(event) {
    const nodeName = event.name;
    const renderNode = this.renderHierarchy.getRenderNodeByName(nodeName);

    if (renderNode.node.type === NodeType.OP) {
      return;
    }

    this.renderHierarchy.buildSubhierarchy(nodeName);
    renderNode.expanded = !renderNode.expanded;

    this.event.emit({
      eventName: 'node-toggle-expand',
      event: renderNode
    });

    this.build();
  }

  clean() {
    this._nodeGroupIndex = {};
    this._edgeGroupIndex = [];
    this.portalCacheMap.forEach(v => {
      v.host.dispose();
      v.temp = null;
    });
    this.portalCacheMap.clear();
    if (this.sceneGroup) {
      this.sceneGroup.selectAll('*').remove();
    }
  }

  build() {
    requestAnimationFrame(() => {
      this.layoutScene(this.renderHierarchy.root);
      this.buildGroupScene(this.renderHierarchy.root);
      setTimeout(() => {
        if (this.selectedNode) {
          traceInputs(this.renderHierarchy);
        }
        if (this.minmap) {
          this.minmap.update();
        }
      }, 250);
    });
  }

  fit() {
    if (this.zoom) {
      this.zoom.fit();
    }
  }

  panToCenterByNodeName(nodeName: string) {
    const nodeElement = getNodeElementByName(nodeName);
    if (nodeElement && this.zoom) {
      requestAnimationFrame(() => {
        this.zoom.panToCenter(nodeElement as SVGGElement);
      });
    }
  }

  fire(eventName, event?) {
    this.event.emit({eventName, event});
    switch (eventName) {
      case 'node-toggle-expand':
        this.nodeToggleExpand(event);
        break;
      case 'edge-select':
        this.handleEdgeSelected(event);
        break;
      case 'node-select':
        this.handleNodeSelected(event);
        break;
      default:
        break;
    }
  }

  handleEdgeSelected(event) {
    console.log(event);
  }

  handleNodeSelected(event) {
    if (!event.name) {
      return;
    }
    const lastSelect = this.selectedNode;
    this.selectedNode = event.name;
    this.updateNodeState(event.name);
    if (lastSelect) {
      this.updateNodeState(lastSelect);
    }
    this.traceInputs();
  }

  traceInputs() {
    traceInputs(this.renderHierarchy);
  }

  isNodeExpanded(e) {

  }

  addNodeGroup(node, selection) {
    this._nodeGroupIndex[node] = selection;
  }

  removeNodeGroup(node) {
    delete this._nodeGroupIndex[node];
  }

  getNodeGroup(node) {
    return this._nodeGroupIndex[node];
  }

  getNode(node) {
    return this.renderHierarchy.getRenderNodeByName(node);
  }

  removeNodeGroupPortal(renderNodeInfo: RenderGroupNodeInfo) {
    if (this.portalCacheMap.has(renderNodeInfo)) {
      const portal = this.portalCacheMap.get(renderNodeInfo);
      portal.host.detach();
      portal.temp = null;
      portal.host = null;
      this.portalCacheMap.delete(renderNodeInfo);
    }
  }

  createInjector(renderNodeInfo: RenderGroupNodeInfo) {
    const injectorTokens = new WeakMap();
    injectorTokens.set(RENDER_NODE_INFO, renderNodeInfo);
    injectorTokens.set(RENDER_NODE_INFO_CHANGE, new Subject< RenderNodeInfo | RenderGroupNodeInfo>());
    injectorTokens.set(NzGraphComponent, this);
    return new PortalInjector(this.injector, injectorTokens);
  }

  emitChangeByNodeInfo(renderNodeInfo: RenderNodeInfo | RenderGroupNodeInfo) {
    const portalCache = this.portalCacheMap.get(renderNodeInfo);
    if (portalCache) {
      (portalCache.temp.injector.get(RENDER_NODE_INFO_CHANGE) as Subject< RenderNodeInfo | RenderGroupNodeInfo>).next(renderNodeInfo);
    }
  }

  addNodePortal(element: d3.Selection<any, any, any, any>, renderNodeInfo: RenderGroupNodeInfo) {
    if (!this.nodePortal) {
      throw new Error('没有找到 nodePortal');
    }

    // BRIDGE 不需要定义
    if (renderNodeInfo.node.type === NodeType.BRIDGE) {
      return;
    }

    // 嵌入模版的容器
    const nodeForeignObject = element.select('foreignObject').node() as HTMLElement;
    const injector = this.createInjector(renderNodeInfo);
    const portalCache = this.portalCacheMap.get(renderNodeInfo);
    // 是否被添加过
    if (this.portalCacheMap.has(renderNodeInfo)) {
      // 如果被添加过但是当前容器中却不存在之前的模版则重新添加（因为被收起或其他原因被移除）
      if (!(element.node() as HTMLElement).contains(portalCache.host.outletElement)) {
        portalCache.host.dispose();
        portalCache.host = null;
        portalCache.temp = null;
        this.portalCacheMap.delete(renderNodeInfo);
      } else {
        (portalCache.temp.injector.get(RENDER_NODE_INFO_CHANGE) as Subject<RenderGroupNodeInfo>).next(renderNodeInfo);
        return;
      }
    }

    const nodePortalHost = new DomPortalHost(
      nodeForeignObject,
      this.componentFactoryResolver,
      this.appRef,
      injector
    );

    const portal = new ComponentPortal(
      this.nodePortal,
      this.viewContainerRef,
      injector,
      // this.componentFactoryResolver // CDK 7.v
    );

    let componentInstance = nodePortalHost.attach(portal);
    componentInstance.changeDetectorRef.detectChanges();
    componentInstance = null;
    this.portalCacheMap.set(renderNodeInfo, {
      host: nodePortalHost,
      temp: portal
    });
  }


  isNodeHighlighted(nodeName) {
    return nodeName === this.selectedNode;
  }

  isNodeSelected(nodeName) {
    return nodeName === this.selectedNode;
  }

  ngOnInit() {
  }

  ngAfterContentInit(): void {
    this.bindZoom();
  }

  ngOnDestroy(): void {
    if (this.zoom) {
      this.zoom.unbind();
    }
  }

  bindZoom() {
    this.zoom = new DagreZoom(this.$root.nativeElement, this.$svg.nativeElement);
    if (this.minmap) {
      this.zoom.transformChange.subscribe((e) => this.minmap.zoom(e));
    }
  }

  updateNodeState(nodeName: string): void {
    if (!nodeName) {
      return;
    }
    const nodeGroup = this.getNodeGroup(nodeName);
    const node = this.getNode(nodeName);
    if (nodeGroup) {
      stylize(nodeGroup, node, this);
    }
  }

  expandParents(nodeName: string): boolean {
    let node = this.renderHierarchy.hierarchy.node(nodeName) as Node;
    if (!node) {
      return;
    }
    // 存放它的所有父节点
    const nodeParents = [];
    // 找到所有除了 root 之外的的所有父节点
    while (node.parentNode !== null && node.parentNode.name !== ROOT_NAME) {
      node = node.parentNode;
      nodeParents.push(node.name);
    }
    // 用于标记之前未展开的父节点，如果都展开了它则为空，将不会重新进行渲染
    let topParentNodeToBeExpanded;
    // 倒序展开
    _.forEachRight(nodeParents, (parentName) => {
      this.renderHierarchy.buildSubhierarchy(parentName);
      const renderNode = this.renderHierarchy.getRenderNodeByName(parentName);
      if (renderNode.node.isGroupNode && !renderNode.expanded) {
        renderNode.expanded = true;
        if (!topParentNodeToBeExpanded) {
          topParentNodeToBeExpanded = renderNode;
        }
      }
    });
    return topParentNodeToBeExpanded;
  }

  selectedNodeChanged(nodeName: string) {
    if (!nodeName || nodeName === this.selectedNode) {
      return;
    }
    const topParentNodeToBeExpanded = this.expandParents(nodeName);
    // 确保存在展开状态存在变换的节点
    if (topParentNodeToBeExpanded) {
      this.build();
    }


    // buildDef 是异步的
    setTimeout(() => {
      // 展开后将剧中当前节点，在 node 很多时，250ms 可能不够。
      this.panToCenterByNodeName(nodeName);
      this.handleNodeSelected({
        name: nodeName
      });
    }, 250);
  }

  expandOrCollapseAll(expand = true) {
    const nodeMap = this.renderHierarchy.hierarchy.getNodeMap();
    const groupNodes = [];
    let isBuild = false;
    Object.keys(nodeMap).forEach(key => {
      if (nodeMap[key].isGroupNode && nodeMap[key].name !== ROOT_NAME) {
        groupNodes.push(key);
      }
    });
    groupNodes.forEach(name => {
      this.renderHierarchy.buildSubhierarchy(name);
      const renderNode = this.renderHierarchy.getRenderNodeByName(name);
      if (renderNode.node.isGroupNode && expand !== renderNode.expanded) {
        renderNode.expanded = expand;
        if (!isBuild) {
          isBuild = true;
        }
      }
    });

    if (isBuild) {
      this.build();
    }
  }

}
