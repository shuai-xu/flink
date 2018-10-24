import { ChangeDetectorRef, Component, OnInit, ViewChild, ChangeDetectionStrategy } from '@angular/core';
import { JobManagerService } from '../../../services/job-manager.service';
import { MonacoEditorComponent } from '../../../share/monaco-editor/monaco-editor.component';

@Component({
  selector       : 'flink-job-manager-logs',
  templateUrl    : './job-manager-logs.component.html',
  changeDetection: ChangeDetectionStrategy.OnPush,
  styleUrls      : [ './job-manager-logs.component.less' ]
})
export class JobManagerLogsComponent implements OnInit {
  @ViewChild(MonacoEditorComponent) monacoEditorComponent: MonacoEditorComponent;
  logs = '';

  constructor(private jobManagerService: JobManagerService, private cdr: ChangeDetectorRef) {
  }

  ngOnInit() {
    this.jobManagerService.loadLogs().subscribe(data => {
      this.monacoEditorComponent.layout();
      this.logs = data;
      this.cdr.markForCheck();
    });
  }

}
