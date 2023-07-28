<script setup lang="ts">
// 导入 invoke 方法
import {invoke} from '@tauri-apps/api/tauri'
import {ElConfigProvider} from 'element-plus'
import CustomMenuList from './components/MenuList.vue'

import {
    ref,
    computed,
    reactive,
onMounted,
onBeforeMount
} from "vue";
import {menus} from "./route/menu"




onMounted(() => {
    // DOM 内容加载完成之后，关闭 splashscreen
    setTimeout(() => invoke('close_splashscreen'), 2000)
})



</script>

<template>
   <el-row class="tac">
    <el-col :span="4">
      <CustomMenuList :menus="menus"></CustomMenuList>
    </el-col>
    <el-col :span="18">
        <router-view></router-view>
    </el-col>
  </el-row>

</template>

<style scoped>

</style>
