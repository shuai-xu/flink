import { Component, OnInit } from '@angular/core';
import { first, flatMap } from 'rxjs/operators';
import { TaskManagerService } from 'services';

@Component({
  selector   : 'flink-task-manager-log-list',
  templateUrl: './task-manager-log-list.component.html',
  styleUrls  : [ './task-manager-log-list.component.less' ]
})
export class TaskManagerLogListComponent implements OnInit {
  listOfLog = [];
  isLoading = true;

  constructor(private taskManagerService: TaskManagerService) {
  }

  ngOnInit() {
    this.taskManagerService.taskManagerDetail$.pipe(
      first(),
      flatMap(() => this.taskManagerService.loadLogList(this.taskManagerService.taskManagerDetail.id))
    ).subscribe(data => {
      this.listOfLog = data.logs;
      this.isLoading = false;
    }, () => this.isLoading = false);
  }

}
