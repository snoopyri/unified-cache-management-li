# *** YuanrongStore构建脚本：依赖Ascend ACL和datasystem库
# *** 条件编译：仅在找到所有依赖（ACL头文件、ACL库、datasystem库）时才构建
# *** 缺少依赖时跳过构建并提示安装
find_path(ASCEND_ACL_INCLUDE_DIR NAMES acl/acl.h PATHS /usr/local/Ascend/ascend-toolkit/latest/include NO_DEFAULT_PATH)  # *** 查找Ascend ACL头文件
find_library(ASCEND_ACL_LIBRARY NAMES ascendcl PATHS /usr/local/Ascend/ascend-toolkit/latest/lib64 NO_DEFAULT_PATH)  # *** 查找Ascend ACL库
find_path(DATASYSTEM_INCLUDE_DIR NAMES datasystem/hetero_client.h PATHS /usr/local/include NO_DEFAULT_PATH)  # *** 查找datasystem头文件
find_library(DATASYSTEM_LIBRARY NAMES datasystem PATHS /usr/local/lib NO_DEFAULT_PATH)  # *** 查找datasystem库

if(ASCEND_ACL_INCLUDE_DIR AND ASCEND_ACL_LIBRARY AND DATASYSTEM_INCLUDE_DIR AND DATASYSTEM_LIBRARY)  # *** 所有依赖都找到时才构建
    file(GLOB_RECURSE SOURCES "./cc/*.cc")  # *** 递归收集所有C++源文件
    add_library(yuanrongstore SHARED ${SOURCES})  # *** 构建为动态库

    target_compile_features(yuanrongstore PUBLIC cxx_std_20)  # *** 要求C++20标准

    # *** 包含目录：项目源码、datasystem头文件
    target_include_directories(yuanrongstore PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/cc
        ${DATASYSTEM_INCLUDE_DIR}
    )
    # *** 系统包含目录：Ascend ACL头文件（SYSTEM标记抑制警告）
    target_include_directories(yuanrongstore SYSTEM PUBLIC
        ${ASCEND_ACL_INCLUDE_DIR}
    )

    # *** 链接库：UCM内部库(storeintf/infra_logger/fmt/trans) + datasystem + Ascend ACL
    target_link_libraries(yuanrongstore PUBLIC
        storeintf infra_logger fmt trans ${DATASYSTEM_LIBRARY}
        ${ASCEND_ACL_LIBRARY}
    )

    file(RELATIVE_PATH INSTALL_REL_PATH ${UCM_ROOT_DIR} ${CMAKE_CURRENT_SOURCE_DIR})
    install(TARGETS yuanrongstore LIBRARY DESTINATION ${INSTALL_REL_PATH} COMPONENT ucm)  # *** 安装到相对路径
else()  # *** 缺少依赖时跳过构建
    message(STATUS "yuanrongstore: Skipping build - required Ascend/datasystem dependencies not found")
    if(NOT ASCEND_ACL_INCLUDE_DIR)
        message(STATUS "  * Missing: Ascend ACL headers (acl/acl.h)")
    endif()
    if(NOT ASCEND_ACL_LIBRARY)
        message(STATUS "  * Missing: libascendcl.so library")
    endif()
    if(NOT DATASYSTEM_INCLUDE_DIR)
        message(STATUS "  * Missing: datasystem headers (datasystem/hetero_client.h)")
    endif()
    if(NOT DATASYSTEM_LIBRARY)
        message(STATUS "  * Missing: libdatasystem.so library")
    endif()
    message(STATUS "  Please ensure Ascend and yuanrong-datasystem dependencies are installed or build paths are correct")
endif()