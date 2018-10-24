import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { NgZorroAntdModule } from 'ng-zorro-antd';
import { PipeModule } from '../pipes/pipe.module';
import { DagreModule } from './dagre/dagre.module';
import { LayoutComponent } from './layout/layout.component';
import { TaskBadgeComponent } from './task-badge/task-badge.component';
import { JobBadgeComponent } from './job-badge/job-badge.component';
import { JobListComponent } from './job-list/job-list.component';
import { TaskDescriptionComponent } from './task-description/task-description.component';
import { MonacoEditorComponent } from './monaco-editor/monaco-editor.component';
import { NavigationComponent } from './navigation/navigation.component';
import { FileReadDirective } from './file-read/file-read.directive';
import { ResizeComponent } from './resize/resize.component';

@NgModule({
  imports     : [
    CommonModule,
    NgZorroAntdModule,
    PipeModule,
    RouterModule,
    DagreModule
  ],
  declarations: [
    LayoutComponent,
    TaskBadgeComponent,
    JobBadgeComponent,
    JobListComponent,
    TaskDescriptionComponent,
    MonacoEditorComponent,
    NavigationComponent,
    FileReadDirective,
    ResizeComponent
  ],
  exports     : [
    LayoutComponent,
    TaskBadgeComponent,
    JobBadgeComponent,
    JobListComponent,
    TaskDescriptionComponent,
    DagreModule,
    MonacoEditorComponent,
    NavigationComponent,
    FileReadDirective,
    ResizeComponent
  ]
})
export class ShareModule {
}
