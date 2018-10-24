import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { HumanizeDurationPipe } from './humanize-duration.pipe';
import { HumanizeBytesPipe } from './humanize-bytes.pipe';
import { HumanizeWatermarkPipe } from './humanize-watermark.pipe';

@NgModule({
  imports     : [
    CommonModule
  ],
  declarations: [
    HumanizeDurationPipe,
    HumanizeBytesPipe,
    HumanizeWatermarkPipe
  ],
  exports     : [
    HumanizeDurationPipe,
    HumanizeBytesPipe,
    HumanizeWatermarkPipe
  ]
})
export class PipeModule {
}
