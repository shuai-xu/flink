import { AfterViewInit, Component, ElementRef, Input, OnDestroy, OnInit, ViewChild, ChangeDetectionStrategy } from '@angular/core';
import * as G2 from '@antv/g2';
import { Subject } from 'rxjs';
import { filter, flatMap, startWith, takeUntil } from 'rxjs/operators';
import { INodeCorrect } from '../../../../interfaces/job';
import { JobService } from '../../../../services/job.service';
import { MetricsService } from '../../../../services/metrics.service';
import { StatusService } from '../../../../services/status.service';

@Component({
  selector       : 'flink-job-overview-drawer-chart',
  templateUrl    : './job-overview-drawer-chart.component.html',
  changeDetection: ChangeDetectionStrategy.OnPush,
  styleUrls      : [ './job-overview-drawer-chart.component.less' ]
})
export class JobOverviewDrawerChartComponent implements AfterViewInit, OnInit, OnDestroy {
  destroy$ = new Subject();
  data = [];
  listOfMetricName = [];
  chartInstance;
  selectedMetricName;
  _node: INodeCorrect;
  @ViewChild('chart') chart: ElementRef;

  @Input()
  set node(value: INodeCorrect) {
    if (this._node && (value.id !== this._node.id)) {
      this.loadMetricList();
    }
    this._node = value;
  }

  get node() {
    return this._node;
  }

  clearChart() {
    this.data = [];
    if (this.chartInstance) {
      this.chartInstance.render();
    }
  }

  loadMetricList() {
    this.metricsService.getAllAvailableMetrics(this.jobService.jobDetail.jid, this.node.id).subscribe(data => {
      this.listOfMetricName = data.map(item => item.id);
      if (this.listOfMetricName && this.listOfMetricName.length) {
        this.selectedMetricName = this.listOfMetricName[ 0 ];
        this.clearChart();
      }
    });
  }

  constructor(private statusService: StatusService, private metricsService: MetricsService, private jobService: JobService) {
  }

  ngOnInit() {
    this.loadMetricList();
    this.statusService.refresh$.pipe(
      startWith(true),
      takeUntil(this.destroy$),
      filter(() => this.selectedMetricName),
      flatMap(() => this.metricsService.getMetrics(this.jobService.jobDetail.jid, this.node.id, [ this.selectedMetricName ]))
    ).subscribe((res) => {
      if (this.chartInstance) {
        this.data.push({
          time : res.timestamp,
          value: res.values[ this.selectedMetricName ],
          type : this.selectedMetricName
        });

        if (this.data.length > 20) {
          this.data.shift();
        }
        this.chartInstance.changeData(this.data);
      }
    });
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }

  ngAfterViewInit() {
    G2.track(false);
    this.chartInstance = new G2.Chart({
      container: this.chart.nativeElement,
      forceFit : true,
      height   : 380
    });
    this.chartInstance.source(this.data, {
      time: {
        alias: 'Time',
        type : 'time',
        mask : 'HH:mm:ss',
        nice : false
      },
      type: {
        type: 'cat'
      }
    });
    this.chartInstance.legend({
      position: 'bottom',
      itemGap : 20
    });
    this.chartInstance.line().position('time*value').shape('smooth').color('type').size(2).animate({
      update: {
        duration: 0
      }
    });
    this.chartInstance.render();
  }

}
