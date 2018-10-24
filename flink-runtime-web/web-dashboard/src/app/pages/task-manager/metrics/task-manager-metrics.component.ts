import { Component, OnInit } from '@angular/core';
import { TaskManagerService } from '../../../services/task-manager.service';

@Component({
  selector   : 'flink-task-manager-metrics',
  templateUrl: './task-manager-metrics.component.html',
  styleUrls  : [ './task-manager-metrics.component.less' ]
})
export class TaskManagerMetricsComponent implements OnInit {

  get detail() {
    return this.taskManagerService.taskManagerDetail;
  }

  constructor(private taskManagerService: TaskManagerService) {
  }

  ngOnInit() {
  }

}
