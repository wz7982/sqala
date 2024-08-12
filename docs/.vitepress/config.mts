import { defineConfig } from 'vitepress';
import { fetchItems } from './fetch_sidebar.mts';

export default defineConfig({
  title: 'sqala',
  description: 'sqala the sql lang in scala',
  base: '/sqala',
  rewrites: {
    'zh/:rest*': ':rest*',
  },
  themeConfig: {
    socialLinks: [{ icon: 'github', link: 'https://github.com/wz7982/sqala' }],
    search: {
      provider: 'local',
    },
  },
  locales: {
    root: {
      label: '简体中文',
      lang: 'zh-Hans',
      themeConfig: {
        langMenuLabel: '多语言',
        nav: [{ text: '主页', link: '/' }],
        sidebar: {
          '/': {
            base: '/',
            items: fetchItems('zh'),
          },
        },
      },
    },
    en: {
      label: 'English',
      lang: 'en-US',
      link: '/en/',
      themeConfig: {
        nav: [{ text: 'Home', link: '/en/' }],
        sidebar: {
          '/en/': {
            base: '/en/',
            items: fetchItems('en'),
          },
        },
      },
    },
  },
});
