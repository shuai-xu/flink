import { Component, Input, OnInit, ChangeDetectionStrategy } from '@angular/core';
import { INodeCorrect } from '../../../../interfaces/job';

@Component({
  selector       : 'flink-job-overview-drawer-detail',
  templateUrl    : './job-overview-drawer-detail.component.html',
  changeDetection: ChangeDetectionStrategy.OnPush,
  styleUrls      : [ './job-overview-drawer-detail.component.less' ]
})
export class JobOverviewDrawerDetailComponent implements OnInit {
  @Input() node: INodeCorrect;

  constructor() {
  }

  ngOnInit() {
  }
}
