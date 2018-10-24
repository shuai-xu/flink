import { curveLinear, line, scaleThreshold } from 'd3';
import * as dagre from 'dagre';
import { GraphEdge, graphlib } from 'dagre';
import { INodeCorrect, INodeLink } from '../../interfaces/job';
import Graph = graphlib.Graph;

export interface LayoutNode extends INodeCorrect {
  x?: number;
  y?: number;
  width?: number;
  height?: number;
  options: LayoutNodeOptions;
}

export interface LayoutNodeOptions {
  transform: string;
  oldTransform: string;
  scale: number;
  oldScale: number;
  focused: boolean;
}

export interface LayoutLink extends INodeLink {
  [ key: string ]: any;

  options: LayoutLinkOptions;
  points?: Array<{ x: number, y: number }>;
}


export interface LayoutLinkOptions {
  line: string;
  oldLine: string;
  width: number;
  oldWidth: number;
  focused: boolean;
  dominantBaseline: string;
}

export interface CreateGraphOpt {
  directed?: boolean;
  multigraph?: boolean;
  compound?: boolean;
}

export interface CircleLayoutOpt {
  nodeId: string;
  zoom: number;
  x: number;
  y: number;
  viewportExpanse: number;
}

export interface ZoomFocusLayoutOpt {
  nodeId: string;
  zoom: number;
  x: number;
  y: number;
  transform: { x: number; y: number; k: number };
}

export enum NodeShape {
  Circle,
  Rect
}

export class NzGraph {

  graph: Graph;
  config = {
    ranker : 'network-simplex',
    align  : 'DL',
    marginx: 20,
    marginy: 20,
    edgesep: 150,
    ranksep: 150
  };

  copyLayoutNodes: LayoutNode[];
  layoutNodes: LayoutNode[] = [];

  copyLayoutLinks: LayoutLink[];
  layoutLinks: LayoutLink[] = [];

  /**
   * 创建 Graph 对象
   */
  createGraph(opt: CreateGraphOpt = {}) {
    this.graph = new dagre.graphlib.Graph(opt);
    this.graph.setGraph({
      rankdir: 'LR',
      ...this.config
    } as any);
    this.graph.setDefaultEdgeLabel(() => ({}));
  }

  /**
   * 设置 Nodes
   */
  setNodes(nodes: INodeCorrect[]) {
    nodes.forEach(n => {
      n[ 'width' ] = n[ 'width' ] || 48;
      n[ 'height' ] = n[ 'height'] || 48;
      this.graph.setNode(n.id, n as any);
    });
  }

  /**
   * 设置 Links
   */
  setEdge(links: INodeLink[]) {
    links.forEach(l => {
      l[ 'width' ] = l[ 'ship_strategy' ].length * 3;
      this.graph.setEdge(l.source, l.target, l);
    });
  }

  /**
   * 初始化布局
   */
  initLayout(nodeShape = NodeShape.Circle) {
    if (!this.graph) {
      return;
    }

    this.layoutNodes = [];
    this.copyLayoutNodes = [];
    this.layoutLinks = [];
    this.copyLayoutLinks = [];

    dagre.layout(this.graph);
    if (this.graph.graph().width < this.graph.graph().height) {
      this.graph.setGraph({
        rankdir: 'TB',
        ...this.config
      });
      this.graph.edges().forEach(e => {
        const edge = this.graph.edge(e);
        edge.height = edge.width;
        edge.width = null;
      });
      dagre.layout(this.graph);
    }

    this.graph.nodes().forEach(id => {
      const node: LayoutNode = this.graph.node(id) as LayoutNode;
      if (nodeShape === NodeShape.Circle) {
        node.width = 1;
        node.height = 1;
      }
      node.options = {
        transform   : `translate(${node.x - node.width / 2 || 0}, ${node.y - 1 / 2 || 0})`,
        oldTransform: `translate(${node.x - node.height / 2 || 0}, ${node.y - 1 / 2 || 0})`,
        scale       : 1,
        oldScale    : 1,
        focused     : false
      };

      this.layoutNodes.push({ ...node, options: { ...node.options } });
      this.copyLayoutNodes.push({ ...node, options: { ...node.options } });
    });

    this.graph.edges().forEach(e => {
      const edge = this.graph.edge(e) as (LayoutLink & GraphEdge);
      const initLine = this.generateLine(edge.points) as string;
      const link: LayoutLink = {
        id     : edge.id,
        source : edge.source,
        target : edge.target,
        points : [ ...edge.points ] as Array<{ x: number, y: number }>,
        options: {
          line            : initLine,
          oldLine         : initLine,
          width           : 1,
          oldWidth        : 1,
          focused         : false,
          dominantBaseline: this.getDominantBaseline(edge)
        },
        detail : { ...edge }
      };
      this.layoutLinks.push({ ...link, options: { ...link.options } });
      this.copyLayoutLinks.push({ ...link, options: { ...link.options } });
    });

    return Promise.resolve();
  }

  getDominantBaseline(edge: GraphEdge) {
    const firstPoint = edge.points[ 0 ];
    const lastPoint = edge.points[ edge.points.length - 1 ];
    return lastPoint.x < firstPoint.x ? 'rtl' : 'ltr';
  }

  /**
   * 聚焦缩放布局
   */
  zoomFocusLayout(opt: ZoomFocusLayoutOpt) {
    if (!this.graph.hasNode(opt.nodeId)) {
      console.warn(`没有这个 node ${opt.nodeId}`);
      return;
    }

    this.layoutNodes.forEach(node => {
      const oNode = this.copyLayoutNodes.find(n => n.id === node.id);
      node.options.oldScale = node.options.scale;
      node.options.scale = oNode.options.scale;
      node.options.focused = false;
    });
    const focusNode = this.layoutNodes.find(n => n.id === opt.nodeId);
    const circularNodes = this.circleNodes(focusNode);
    focusNode.options.oldScale = focusNode.options.scale;
    focusNode.options.scale = focusNode.options.oldScale * 1.2;
    focusNode.options.focused = true;
    const x = focusNode.x + 45;
    const y = focusNode.y;

    const focusedLinkIds = [];
    this.layoutLinks.forEach(link => {
      link.options.focused = link.source === opt.nodeId || link.target === opt.nodeId;
      if (link.options.focused) {
        focusedLinkIds.push(link.id);
      }
    });

    return Promise.resolve({
      focusedLinkIds,
      circularNodeIds: circularNodes.map(n => n.id),
      transform: {
        x: opt.transform.x + opt.x - x,
        y: opt.transform.y + opt.y - y,
        k: 1
      }
    });
  }

  /**
   * 环绕布局
   */
  circleLayout(opt: CircleLayoutOpt) {

    if (!this.graph.hasNode(opt.nodeId)) {
      console.warn(`没有这个 node ${opt.nodeId}`);
      return;
    }

    // 中央的 node
    const centerNode = this.layoutNodes.find(n => n.id === opt.nodeId);

    // 环绕的 nodes
    const circularNodes = this.circleNodes(centerNode);
    const ids = [ centerNode.id, ...circularNodes.map(n => n.id) ];

    // 重置无关的 nodes 布局
    this.layoutNodes.forEach(node => {

      // 初始化时的 node
      const oNode = this.copyLayoutNodes.find(n => n.id === node.id);
      if (oNode && ids.indexOf(node.id) === -1) {
        if (node.options.transform !== oNode.options.transform) {
          node.options.oldTransform = `${node.options.transform}`;
          node.options.oldScale = node.options.scale;
          node.options.transform = `${oNode.options.transform}`;
          node.options.scale = oNode.options.scale;
        } else {
          node.options.oldTransform = `${node.options.transform}`;
          node.options.oldScale = node.options.scale;
        }
        node.x = oNode.x;
        node.y = oNode.y;
      }
      node.options.focused = false;
    });


    /**
     * 中央 node 布局
     */
    centerNode.x = opt.x;
    centerNode.y = opt.y;

    const x = centerNode.x - centerNode.width / 2;
    const y = centerNode.y - centerNode.height / 2;
    const oldCenterNode = this.copyLayoutNodes.find(n => n.id === centerNode.id);
    const scaleLevel = 1 / opt.zoom * 1.2;
    // 适应当前缩放
    centerNode.options.oldScale = centerNode.options.scale;
    centerNode.options.scale = oldCenterNode.options.scale * scaleLevel;
    centerNode.options.oldTransform = centerNode.options.transform;
    const offsetX = (1 - centerNode.options.scale) * (centerNode.width / 2);
    const offsetY = (1 - centerNode.options.scale) * (centerNode.height / 2);
    centerNode.options.transform = `translate(${x + offsetX}, ${y + offsetY})`;


    /**
     * 环绕 nodes 布局
     */
    const circularNodesCount = circularNodes.length;
    const circularOffsetAngle = Math.PI / 4;
    const radiusDensity = scaleThreshold().domain([ 3, 6 ] as ReadonlyArray<number>).range([ 2.5, 3, 2.5 ] as ReadonlyArray<number>);
    const circularRadius = opt.viewportExpanse / radiusDensity(circularNodesCount) * scaleLevel;
    const circularInnerAngle = (2 * Math.PI) / circularNodesCount;
    circularNodes.forEach((node, index) => {
      const angle = circularOffsetAngle + (index * circularInnerAngle);
      const oNode = this.copyLayoutNodes.find(n => n.id === node.id);
      node.x = centerNode.x + (circularRadius * Math.sin(angle));
      node.y = centerNode.y + (circularRadius * Math.cos(angle));
      node.options.focused = true;
      const cx = node.x - node.width / 2;
      const cy = node.y - node.height / 2;
      node.options.oldTransform = node.options.transform;
      node.options.oldScale = node.options.scale;
      node.options.scale = oNode.options.scale * scaleLevel * .5;
      const cOffsetX = (1 - node.options.scale) * (node.width / 2);
      const cOffsetY = (1 - node.options.scale) * (node.height / 2);
      node.options.transform = `translate(${cx + cOffsetX}, ${cy + cOffsetY})`;
    });

    /**
     * 重新计算连线
     */
    const focusedLinkIds = [];
    this.layoutLinks.forEach(link => {
      link.options.focused = link.source === centerNode.id || link.target === centerNode.id;

      if (link.options.focused) {
        link.options.oldWidth = link.options.width;
        link.options.width = scaleLevel;
      } else {
        link.options.oldWidth = link.options.width;
        link.options.width = 1;
      }

      if (link.options.focused || ids.indexOf(link.source) !== -1 || ids.indexOf(link.target) !== -1) {
        const sourceNode = this.layoutNodes.find(n => n.id === link.source);
        const targetNode = this.layoutNodes.find(n => n.id === link.target);
        const startingPoint = {
          x: sourceNode.x,
          y: sourceNode.y
        };
        const endingPoint = {
          x: targetNode.x,
          y: targetNode.y
        };

        link.points = [ startingPoint, ...new Array(link.points.length - 2).fill(startingPoint), endingPoint ];
        const newLine = this.generateLine(link.points);
        link.options.oldLine = link.options.line;
        link.options.line = newLine;
        if (link.options.focused) {
          focusedLinkIds.push(link.id);
        }
      } else {
        const oldLink = this.copyLayoutLinks.find(ol => ol.id === link.id);
        link.options.oldLine = link.options.line;
        link.options.line = oldLink.options.line;
        link.points = [ ...oldLink.points ];
      }
    });

    return Promise.resolve({
      circularNodeIds: circularNodes.map(n => n.id),
      focusedLinkIds : focusedLinkIds
    });
  }

  /**
   * 恢复初始布局
   */
  recoveryLayout() {
    this.layoutNodes.forEach(node => {
      const oNode = this.copyLayoutNodes.find(n => n.id === node.id);
      node.options.oldTransform = node.options.transform;
      node.options.transform = oNode.options.transform;
      node.options.oldScale = node.options.scale;
      node.options.scale = oNode.options.scale;
      node.x = oNode.x;
      node.y = oNode.y;
      node.options.focused = false;
    });

    this.layoutLinks.forEach(link => {
      link.options.focused = false;
      const oldLink = this.copyLayoutLinks.find(ol => ol.id === link.id);
      link.points = [ ...oldLink.points ];
      link.options.oldLine = link.options.line;
      link.options.line = oldLink.options.line;
      link.options.oldWidth = link.options.width;
      link.options.width = 1;
    });
    return Promise.resolve();
  }


  circleNodes(selectedNode: LayoutNode) {
    const nodes = [];
    for (const link of this.layoutLinks) {
      if (link.target === selectedNode.id) {
        nodes.push(this.layoutNodes.find(n => n.id === link.source));
      }

      if (link.source === selectedNode.id) {
        nodes.push(this.layoutNodes.find(n => n.id === link.target));
      }
    }
    return nodes;
  }

  generateLine(points): string {
    const lineFunction = line().x((d: any) => d.x).y((d: any) => d.y)
    .curve(curveLinear);
    return lineFunction(points);
  }
}
