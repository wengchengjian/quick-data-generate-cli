import {
  Document,
  Menu as IconMenu,
  Location,
  Setting,
  Search,
  ChatDotRound,
} from "@element-plus/icons-vue";
import { DefineComponent } from "vue";

export interface MenuAttribute {
  name: string;
  path: string;
  icon?: string;
  children?: MenuAttribute[];
}
export const menus: MenuAttribute[] = [
  {
    name: "数据源",
    path: "/datasource",
    icon: "Location",
  },
  {
    name: "数据统计",
    path: "/dashboard",
    icon: "Search",
  },
  {
    name: "日志记录",
    path: "/logging",
    icon: "ChatDotRound",
  },
  {
    name: "配置",
    path: "/settings",
    icon: "Setting",
  },
];
