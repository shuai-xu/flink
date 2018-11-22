import { ChangeDetectorRef, Component, OnInit, ViewChild } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { first, flatMap } from 'rxjs/operators';
import { TaskManagerService } from 'services';
import { MonacoEditorComponent } from 'share/common/monaco-editor/monaco-editor.component';

@Component({
  selector   : 'flink-task-manager-log-detail',
  templateUrl: './task-manager-log-detail.component.html',
  styleUrls  : [ './task-manager-log-detail.component.less' ]
})
export class TaskManagerLogDetailComponent implements OnInit {
  @ViewChild(MonacoEditorComponent) monacoEditorComponent: MonacoEditorComponent;
  logs = '';
  logName = '';

  pageChanged(page) {
    this.taskManagerService.loadLog(this.taskManagerService.taskManagerDetail.id, this.logName, page).subscribe(data => {
      this.logs = data;
      this.cdr.markForCheck();
    });
  }

  constructor(private taskManagerService: TaskManagerService, private cdr: ChangeDetectorRef, private activatedRoute: ActivatedRoute) {
  }

  ngOnInit() {
    this.logName = this.activatedRoute.snapshot.params.logName;
    this.taskManagerService.taskManagerDetail$.pipe(
      first(),
      flatMap(() => this.taskManagerService.loadLog(this.taskManagerService.taskManagerDetail.id, this.logName))
    ).subscribe(data => {
      this.monacoEditorComponent.layout();
      this.logs = data;
      this.cdr.markForCheck();
    });
  }

}
