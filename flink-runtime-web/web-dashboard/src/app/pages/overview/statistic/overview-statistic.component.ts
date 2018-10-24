import { Component, OnDestroy, OnInit } from '@angular/core';
import { Subject } from 'rxjs';
import { flatMap, takeUntil } from 'rxjs/operators';
import { IOverview } from '../../../interfaces/overview';
import { OverviewService } from '../../../services/overview.service';
import { StatusService } from '../../../services/status.service';

@Component({
  selector   : 'flink-overview-statistic',
  templateUrl: './overview-statistic.component.html',
  styleUrls  : [ './overview-statistic.component.less' ]
})
export class OverviewStatisticComponent implements OnInit, OnDestroy {
  statistic = <IOverview>{};
  destroy$ = new Subject();
  isLoading = true;

  constructor(private statusService: StatusService, private overviewService: OverviewService) {
  }

  ngOnInit() {
    this.statusService.refresh$.pipe(
      takeUntil(this.destroy$),
      flatMap(() => this.overviewService.loadOverview())
    ).subscribe(data => {
      this.statistic = data;
      this.isLoading = false;
    });
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }

}
