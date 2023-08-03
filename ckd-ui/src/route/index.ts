// 1. 定义路由组件.
import DataSource from "../components/datasource.vue";
import Settings from "../components/settings.vue";
import DashBoard from "../components/dashboard.vue";
import NotFound from "../components/notFound.vue";
import StaticsLog from "../components/StaticsLog.vue";
import { createRouter, createWebHashHistory } from "vue-router";

// 2. 定义一些路由
// 每个路由都需要映射到一个组件。
const routes = [
    { path: '/', redirect: '/dashboard' },
    { path: '/datasource/:id(\\d+)?', component: DataSource },
    { path: '/settings', component: Settings },
    { path: '/logging', component: StaticsLog },
    { path: '/dashboard', component: DashBoard },
    { path: '/:pathMatch(.*)*', name: 'NotFound', component: NotFound },
]

// 3. 创建路由实例并传递 `routes` 配置
// 你可以在这里输入更多的配置，但我们在这里
// 暂时保持简单
const router = createRouter({
    // 4. 内部提供了 history 模式的实现。为了简单起见，我们在这里使用 hash 模式。
    history: createWebHashHistory(),
    routes, // `routes: routes` 的缩写
})

export default router;