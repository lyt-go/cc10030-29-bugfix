"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import pipeline, resource

# 流水线路由含 /api/pipeline、/api/claims 等更具体的固定前缀，必须放在通用
# 资源路由之后注册，否则 /api/{module} 动态路径会抢先匹配。
ROUTERS = [resource, pipeline]
