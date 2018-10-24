import { Component, Input, OnInit } from '@angular/core';
import { COLOR_MAP } from '../../app.config';
import { IJobStatusCounts } from '../../interfaces/job';

@Component({
  selector   : 'flink-task-description',
  templateUrl: './task-description.component.html',
  styleUrls  : [ './task-description.component.less' ]
})
export class TaskDescriptionComponent implements OnInit {
  @Input() tasks = <IJobStatusCounts>{};
  statusList = Object.keys(COLOR_MAP);

  get colorMap() {
    return COLOR_MAP;
  }

  constructor() {
  }

  ngOnInit() {
  }

}
