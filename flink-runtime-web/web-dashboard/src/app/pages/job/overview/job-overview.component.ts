import { ChangeDetectionStrategy, Component, ElementRef, OnDestroy, OnInit, ViewChild } from '@angular/core';
import { Subject } from 'rxjs';
import { first, skip, takeUntil } from 'rxjs/operators';
import { NodesItemCorrectInterface } from 'interfaces';
import { DagreService, JobService } from 'services';
import { trigger, state, animate, style, transition } from '@angular/animations';
import { NzGraphComponent } from 'share/common/graph';
import { VerticesNodeComponent } from './vertices-node/vertices-node.component';

@Component({
  selector       : 'flink-job-overview',
  templateUrl    : './job-overview.component.html',
  changeDetection: ChangeDetectionStrategy.OnPush,
  providers      : [ DagreService ],
  animations     : [
    trigger('drawer', [
      state('in', style({ transform: 'translateX(0)' })),
      transition('void => *', [
        style({ transform: 'translateX(100%)' }),
        animate(150)
      ]),
      transition('* => void', [
        animate(150, style({ transform: 'translateX(100%)' }))
      ])
    ])
  ],
  styleUrls      : [ './job-overview.component.less' ]
})
export class JobOverviewComponent implements OnInit, OnDestroy {
  nodes = [];
  links = [];
  destroy$ = new Subject();
  selectedNode: NodesItemCorrectInterface;
  top = 500;
  verticesNodeComponent = VerticesNodeComponent;
  @ViewChild(NzGraphComponent) graphComponent: NzGraphComponent;

  onNodeClick(node: NodesItemCorrectInterface, traceInputs = false) {
    this.dagreService.setTransformCache();
    this.panToCenterByNodeName(node.id);
    this.selectedNode = node;
  }

  onListNodeClick(node: NodesItemCorrectInterface) {
    this.graphComponent.fire('node-select', {
      name: node.id
    });
    this.onNodeClick(node);
  }

  onCloseDrawer() {
    this.selectedNode = null;
    setTimeout(() => {
      this.dagreService.resetTransform();
    }, 200);
  }

  panToCenterByNodeName(name: string) {
    setTimeout(() => {
      this.graphComponent.panToCenterByNodeName(name);
    }, 300);
  }

  constructor(private jobService: JobService, private dagreService: DagreService, public elementRef: ElementRef) {
  }

  ngOnInit() {
    this.jobService.jobDetail$.pipe(
      takeUntil(this.destroy$),
      first()
    ).subscribe(data => {
      this.nodes = data.plan.nodes;
      this.links = data.plan.links;
      this.dagreService.initGraph(this.graphComponent, data);
    });
    this.jobService.jobDetail$.pipe(
      takeUntil(this.destroy$),
      skip(1)
    ).subscribe(data => {
      this.dagreService.updateData(data);
      this.nodes = data.plan.nodes;
    });
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }

  dagreEvent($event) {
    switch ($event.eventName) {
      case 'vertices-click':
        this.onNodeClick($event.event);
        break;
      default:
        break;
    }
  }

  collapseAll() {
    this.graphComponent.expandOrCollapseAll(false);
  }

  expandAll() {
    this.graphComponent.expandOrCollapseAll(true);
    setTimeout(() => {
      this.graphComponent.traceInputs();
    }, 250);
  }
}
