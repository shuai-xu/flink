import { Component, Input, OnDestroy, OnInit, ChangeDetectionStrategy, ChangeDetectorRef } from '@angular/core';
import { Subject } from 'rxjs';
import { flatMap, startWith, takeUntil } from 'rxjs/operators';
import { NodesItemCorrectInterface } from 'interfaces';
import { JobService, MetricsService, StatusService } from 'services';

@Component({
  selector       : 'flink-job-overview-drawer-watermarks',
  templateUrl    : './job-overview-drawer-watermarks.component.html',
  changeDetection: ChangeDetectionStrategy.OnPush,
  styleUrls      : [ './job-overview-drawer-watermarks.component.less' ]
})
export class JobOverviewDrawerWatermarksComponent implements OnInit, OnDestroy {
  @Input() node: NodesItemCorrectInterface;
  destroy$ = new Subject();
  listOfWaterMark = [];
  isLoading = true;

  trackWatermarkBy(index, node) {
    return node.subTaskIndex;
  }

  constructor(
    private jobService: JobService,
    private metricsService: MetricsService,
    private statusService: StatusService,
    private cdr: ChangeDetectorRef) {
  }

  ngOnInit() {
    this.statusService.refresh$.pipe(
      startWith(true),
      takeUntil(this.destroy$),
      flatMap(() => this.metricsService.getWatermarks(this.jobService.jobDetail.jid, this.node.id, this.node.parallelism))
    ).subscribe(data => {
      const list = [];
      this.isLoading = false;
      for (const key in data.watermarks) {
        list.push({
          subTaskIndex: key,
          watermark   : data.watermarks[ key ]
        });
      }
      this.listOfWaterMark = list;
      this.cdr.markForCheck();
    }, () => {
      this.isLoading = false;
      this.cdr.markForCheck();
    });
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }

}
