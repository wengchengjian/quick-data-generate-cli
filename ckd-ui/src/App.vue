<script setup lang="ts">
// 导入 invoke 方法
import {invoke} from '@tauri-apps/api/tauri'
import {ElConfigProvider} from 'element-plus'

import zhCn from 'element-plus/dist/locale/zh-cn.mjs'

import {
    ref,
    computed,
    reactive
} from "vue";

const count = ref(0);

const author = reactive({
    name: 'John Doe',
    books: [
        'Vue 2 - Advanced Guide',
        'Vue 3 - Basic Guide',
        'Vue 4 - The Mystery'
    ]
})

function increment() {
    count.value++
}

const publishedBooksMessage = computed(() => {
    return author.books.length > 0 ? 'Yes' : 'No';
});

// 添加监听函数，监听 DOM 内容加载完成事件
document.addEventListener('DOMContentLoaded', () => {
    // DOM 内容加载完成之后，通过 invoke 调用 在 Rust 中已经注册的命令
    setTimeout(() => invoke('close_splashscreen'), 2000)
})

</script>

<template>
    <el-config-provider :locale="zhCn">
        <div class="container">
            <h1>Welcome to Tauri!</h1>


            <p>Click on the Tauri, Vite, and Vue logos to learn more.</p>


            <button @click="increment">
                {{ count }}
            </button>

            <p>Has published books:</p>
            <span>{{ publishedBooksMessage }}</span>
            <router-link to="/">Go to Home</router-link>
            <router-link to="/settings">Go to Settings</router-link>
        </div>

    </el-config-provider>
</template>

<style scoped>

</style>
