import { Injectable } from '@angular/core';
import { GraphDef, NzGraphComponent, Metaedge, RenderGraphInfo, RenderGroupNodeInfo, RenderNodeInfo } from 'share/common/graph';
import { JobDetailCorrectInterface, NodesItemCorrectInterface } from 'interfaces';

import * as d3 from 'd3';

export interface ViewVerticesDetail {
  parallelism: number;
  inQueue: number;
  inQueueDisplay: string;
  outQueue: number;
  outQueueDisplay: string;
  displayName: string;
}

export interface ViewOperatorsDetail {
  numRecordsIn: string;
  numRecordsOut: string;
  displayName: string;
  name: string;
}

const getLabelForEdge = (metaedge: Metaedge,
                         renderInfo: RenderGraphInfo): string => {
  if (Object.keys(renderInfo.getSubhierarchy()).length === 1) {
    return metaedge.baseEdgeList[ 0 ][ 'partitioner' ] || null;
  }
  return null;
};

@Injectable({
  providedIn: 'root'
})
export class DagreService {

  sourceData: JobDetailCorrectInterface;
  graphComponent: NzGraphComponent;
  graphDef: GraphDef;
  verticesDetailsCache = new Map<RenderGroupNodeInfo, ViewVerticesDetail>();
  operatorsDetailsCache = new Map<RenderNodeInfo, ViewOperatorsDetail>();
  transformCache: { x: number, y: number, k: number };

  constructor() {
  }

  cleanDetailCache() {
    this.verticesDetailsCache.clear();
    this.operatorsDetailsCache.clear();
  }

  setTransformCache() {
    if (this.transformCache) {
      return;
    }
    const { x, y, k } = this.graphComponent.zoom.zoomTransform;
    this.transformCache = {
      x,
      y,
      k
    };
  }

  resetTransform() {
    const transform = d3.zoomIdentity
    .scale(this.transformCache.k)
    .translate(this.transformCache.x / this.transformCache.k, this.transformCache.y / this.transformCache.k);

    d3.select(this.graphComponent.zoom.containerEle)
    .transition().duration(500)
    .call(this.graphComponent.zoom.zoom.transform, transform);
    this.transformCache = null;
  }

  initGraph(graphComponent: NzGraphComponent, data: JobDetailCorrectInterface) {
    const graphDef = this.parseGraphData(data);
    this.graphComponent = graphComponent;
    this.cleanDetailCache();
    graphComponent.buildGraph(graphDef)
    .then(graph => graphComponent.buildRenderGraphInfo(graph))
    .then(graphInfo => {
      graphInfo.edgeLabelFunction = getLabelForEdge;
      graphComponent.build();
      setTimeout(() => {
        graphComponent.fit();
      }, 200);
    });
  }

  updateData(data: JobDetailCorrectInterface) {
    this.sourceData = data;
    this.operatorsDetailsCache.forEach((v, k) => {
      Object.assign(v, this.getOperatorsDetail(k, true));
      this.graphComponent.emitChangeByNodeInfo(k);
    });
    this.verticesDetailsCache.forEach((v, k) => {
      Object.assign(v, this.getVerticesDetail(k, true));
      this.graphComponent.emitChangeByNodeInfo(k);
    });
  }

  parseGraphData(data: JobDetailCorrectInterface): GraphDef {
    this.sourceData = data;
    const nodes = [];
    const getNamespaces = operatorId => {
      const op = data.verticesDetail.operators.find(e => e.operator_id === operatorId);
      return `${op.vertex_id}/${op.operator_id}`;
    };
    data.verticesDetail.operators.forEach(op => {
      nodes.push({
        name  : getNamespaces(op.operator_id),
        inputs: op.inputs.map(e => {
          return {
            name: getNamespaces(e.operator_id),
            attr: { ...e }
          };
        }),
        attr  : { ...op }
      });
    });
    this.graphDef = {
      nodes
    };
    return this.graphDef;
  }

  getVerticesDetail(nodeRenderInfo: RenderGroupNodeInfo, force = false): ViewVerticesDetail {
    if (this.verticesDetailsCache.has(nodeRenderInfo) && !force) {
      return this.verticesDetailsCache.get(nodeRenderInfo);
    }
    const vertices = this.sourceData.verticesDetail.vertices.find(v => v.id === nodeRenderInfo.node.name);
    if (!vertices) {
      return null;
    }

    let displayName = '';
    if (vertices.name) {
      displayName = `${vertices.name.substring(0, 125)}...`;
    } else {
      displayName = vertices.name;
    }

    const inQueue = Math.max(
      ...vertices.subtask_metrics
      .map(m => this.parseFloat(m[ 'buffers.inPoolUsage' ])));

    const outQueue = Math.max(
      ...vertices.subtask_metrics
      .map(m => this.parseFloat(m[ 'buffers.outPoolUsage' ])));

    this.verticesDetailsCache.set(nodeRenderInfo, {
      displayName,
      inQueue,
      outQueue,
      parallelism: vertices.subtask_metrics.length,
      inQueueDisplay    : Number.isFinite(inQueue) ? `${inQueue * 100}%` : ' - ',
      outQueueDisplay   : Number.isFinite(outQueue) ? `${outQueue * 100}%` : ' - '
    });

    return this.verticesDetailsCache.get(nodeRenderInfo);
  }

  getOperatorsDetail(nodeRenderInfo: RenderNodeInfo, force = false): ViewOperatorsDetail {
    if (this.operatorsDetailsCache.has(nodeRenderInfo) && !force) {
      return this.operatorsDetailsCache.get(nodeRenderInfo);
    }
    const operators = this.sourceData.verticesDetail.operators.find(o => o.operator_id === nodeRenderInfo.node.attr[ 'operator_id' ]);
    if (!operators) {
      return null;
    }

    let displayName = '';
    if (operators.name) {
      displayName = `${operators.name.substring(0, 512)}...`;
    } else {
      displayName = operators.name;
    }

    const vertices = this.sourceData.verticesDetail.vertices.find(v => v.id === operators.vertex_id);

    const numRecordsIn = Math.max(
      ...vertices.subtask_metrics.map(m => this.parseFloat(m[ `${operators.metric_name}.numRecordsInOperator` ])));
    const numRecordsOut = Math.max(
      ...vertices.subtask_metrics.map(m => this.parseFloat(m[ `${operators.metric_name}.numRecordsOutOperator` ])));
    this.operatorsDetailsCache.set(nodeRenderInfo, {
        displayName: displayName,
        name: operators.name,
        numRecordsIn : Number.isFinite(numRecordsIn) ? `${numRecordsIn}` : ' - ',
        numRecordsOut : Number.isFinite(numRecordsOut) ? `${numRecordsOut}` : ' - '
      }
    );

    return this.operatorsDetailsCache.get(nodeRenderInfo);
  }

  parseFloat(value: number | string): number {
    if (typeof value === 'number') {
      return value;
    } else {
      const n = Number.parseFloat(value);
      return Number.isFinite(n) ? n : 0;
    }
  }

  getNodesItemCorrect(name: string): NodesItemCorrectInterface {
    return this.sourceData.plan.nodes.find(n => n.id === name);
  }
}
