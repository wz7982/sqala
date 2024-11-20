import fs from 'node:fs';
import path from 'node:path';

import type { DefaultTheme } from 'vitepress';

const BASE_DIR = path.resolve('docs');

export function fetchItems(locale: string): DefaultTheme.SidebarItem[] {
  const baseDir = `${BASE_DIR}/${locale}/`;
  const files = fs.readdirSync(baseDir);
  const res: DefaultTheme.SidebarItem[] = [];
  for (const file of files) {
    if (!file.endsWith('md') || file === 'index.md') {
      // filter non markdown files
      continue;
    }
    const curPath = baseDir + file;
    if (fs.lstatSync(curPath).isDirectory()) {
      // filter directory
      continue;
    }
    let fd: number | undefined;
    try {
      fd = fs.openSync(curPath, 'r');
      const content = fs.readFileSync(fd).toString('utf8');
      let firstNonEmpty = '';
      for (const line of content.split('\n')) {
        if (line) {
          firstNonEmpty = line;
          break;
        }
      }
      if (
        !firstNonEmpty ||
        !firstNonEmpty.startsWith('# ') ||
        firstNonEmpty.length < 3
      ) {
        // the first non empty line is not h1 in markdown
        break;
      }
      const title = firstNonEmpty.substring(2);
      res.push({
        text: title,
        link: path.basename(curPath),
      });
    } finally {
      if (fd !== undefined) {
        fs.closeSync(fd);
      }
    }
  }
  return res;
}
