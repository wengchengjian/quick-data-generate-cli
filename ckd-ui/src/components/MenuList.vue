<script setup lang="ts">
// 导入 invoke 方法
import {invoke} from '@tauri-apps/api/tauri'
import { MenuAttribute } from '../route/menu';
import { onMounted } from 'vue';
import CustomMenuList from '../components/MenuList.vue'
import { useRouter } from 'vue-router';
import {menus} from "../route/menu"

defineProps<{"menus": MenuAttribute[], "parentIndex"?: string}>()

onMounted(() => {
  console.log("加载成功");  
})

const router = useRouter();

const isShowChildren = function(menu: MenuAttribute) {
    return menu.children && menu.children.length > 0
}

const getShowIndex = function(currentIndex: string, parentIndex?: string) {
    if(parentIndex) {
        return parentIndex + '' + currentIndex
    }
    return currentIndex;
}

const handleOpen = (key: string, keyPath: string[]) => {
  console.log(key, keyPath)
}
const handleClose = (key: string, keyPath: string[]) => {
  console.log(key, keyPath)
}

const handleSelect = (key: string, keyPath: string[]) => {
    const menu = menus[parseInt(key)];

    router.push(menu.path);
}


</script>

<template>
    <el-menu
        class="el-menu-vertical-demo"
        @open="handleOpen"
        @close="handleClose"
        @select="handleSelect"
      >
      <template v-for="(menu,currentIndex) in menus">
        <el-sub-menu v-if="isShowChildren(menu)"  :index="getShowIndex(currentIndex + '', parentIndex)">
            <template #title>
            <component :is="menu.icon ?? 'Operation'"></component>
                <span>{{ menu.name }}</span>
            </template>
            <custom-menu-list v-if="isShowChildren(menu)" :menus="menu.children ?? []" :parentIndex="currentIndex + ''" ></custom-menu-list>
        </el-sub-menu>
      <el-menu-item v-else :index="getShowIndex(currentIndex + '', parentIndex)">
        <template #title>
            <span>{{ menu.name }}</span>
          </template>
        </el-menu-item>
    </template>
      </el-menu>
    
    
</template>

<style scoped>

</style>
