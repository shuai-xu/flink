export interface IJar {
  address: string;
  files: IJarFile[];
}

export interface IJarFile {
  id: string;
  name: string;
  uploaded: number;
  entry: Array<{
    description: string;
    name: string;
  }>;
}
