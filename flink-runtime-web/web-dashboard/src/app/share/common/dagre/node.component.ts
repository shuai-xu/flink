import {
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  HostListener,
  Input
} from '@angular/core';
import { NodesItemCorrectInterface } from 'interfaces';
import { isNil } from 'lodash';

@Component({
  selector       : '[flink-node]',
  templateUrl    : './node.component.html',
  styleUrls      : [ './node.component.less' ],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class NodeComponent {
  _node = {} as NodesItemCorrectInterface;
  visible = false;
  description = '';
  inQ = 0;
  outQ = 0;

  @Input()
  set node(value) {
    this._node = value;
    const description = this.node.description.replace('&gt;', '>');
    if (description.length > 300) {
      this.description = description.slice(0, 300) + '...';
    } else {
      this.description = description;
    }
    this.inQ = value.detail && value.detail.metrics[ 'buffers-in-pool-usage-max' ];
    this.outQ = value.detail && value.detail.metrics[ 'buffers-out-pool-usage-max' ];

  }

  get node() {
    return this._node;
  }

  @HostListener('click')
  clickNode() {
    this.visible = false;
  }

  get inQDashArray(): string {
    return `${6.283 / 2 * (this.inQ || 0)} 6.283`;
  }

  get outQDashArray(): string {
    return `${6.283 / 2 * (this.outQ || 0)} 6.283`;
  }

  get id() {
    return this.node.id;
  }

  get name() {
    return this.node.description;
  }

  get parallelism() {
    return this.node.parallelism;
  }

  get showInQ() {
    return !isNil(this.inQ);
  }

  get showOutQ() {
    return !isNil(this.outQ);
  }

  constructor(protected cd: ChangeDetectorRef) {
  }

  update(node): void {
    this.node = node;
    this.cd.markForCheck();
  }
}
