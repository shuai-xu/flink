import { ChangeDetectionStrategy, ChangeDetectorRef, Component, Inject, OnDestroy, OnInit } from '@angular/core';
import { Subject, Subscription } from 'rxjs';
import { NzGraphComponent, RENDER_NODE_INFO, RENDER_NODE_INFO_CHANGE, RenderGroupNodeInfo } from 'share/common/graph';
import { DagreService, ViewOperatorsDetail, ViewVerticesDetail } from 'services';

@Component({
  selector: 'flink-vertices-node',
  templateUrl: './vertices-node.component.html',
  styleUrls: ['./vertices-node.component.less'],
  changeDetection    : ChangeDetectionStrategy.OnPush,
  preserveWhitespaces: false
})
export class VerticesNodeComponent implements OnInit, OnDestroy {

  change$ = Subscription.EMPTY;
  nodeselect$ = Subscription.EMPTY;
  verticesDetail: ViewVerticesDetail;
  operatorsDetail: ViewOperatorsDetail;
  constructor(
    @Inject(RENDER_NODE_INFO) public nodeInfo: RenderGroupNodeInfo,
    @Inject(RENDER_NODE_INFO_CHANGE) public nodeInfoChange: Subject<RenderGroupNodeInfo>,
    private graphComponent: NzGraphComponent,
    private cdRef: ChangeDetectorRef,
    private dagreService: DagreService
  ) {
    this.change$ = this.nodeInfoChange.asObservable().subscribe(_ => {
      this.update();
    });

    this.nodeselect$ = this.graphComponent.event.asObservable().subscribe(data => {
      if (data.eventName === 'node-select' && data.event.name === this.nodeInfo.node.name) {
        this.nodeClick();
      }
    });
  }

  ngOnInit() {
    this.update();
  }

  ngOnDestroy(): void {
    this.change$.unsubscribe();
    this.nodeselect$.unsubscribe();
  }

  update() {
    if (this.nodeInfo.node.isGroupNode) {
      this.verticesDetail = this.dagreService.getVerticesDetail(this.nodeInfo);
    } else {
      this.operatorsDetail = this.dagreService.getOperatorsDetail(this.nodeInfo);
    }
    this.cdRef.detectChanges();
  }

  nodeClick() {
    if (!this.nodeInfo.node.isGroupNode) {
      return;
    }
    this.graphComponent.event.emit({
      eventName: 'vertices-click',
      event: this.dagreService.getNodesItemCorrect(this.nodeInfo.node.name)
    });
  }

  toggleExpand($event: MouseEvent) {
    $event.preventDefault();
    $event.stopPropagation();
    this.graphComponent.nodeToggleExpand({
      name: this.nodeInfo.node.name
    });
  }

}
