# Amazon Product Scraper 亚马逊商品爬虫

[English](#english) | [中文](#中文)

---

> ⚠️ **Notice / 声明**
>
> **English:** This project is no longer actively maintained. If you need updates or have questions, feel free to open an issue or submit a pull request.
>
> **中文：** 本项目已停止维护。如有需要更新或问题，欢迎提交 Issue 或 Pull Request。

---

> ⚖️ **Disclaimer / 免责声明**
>
> **English:** This project is for **educational and research purposes only**. The author is not responsible for any misuse of this software. Please ensure compliance with Amazon's Terms of Service and applicable laws before use. Any commercial use or actions that violate the target website's policies are strictly prohibited.
>
> **中文：** 本项目仅供**学习和研究目的**使用。作者不对任何滥用本软件的行为承担责任。使用前请确保遵守亚马逊的服务条款及相关法律法规。严禁任何商业用途或违反目标网站政策的行为。

---

## English

### 📁 Project Structure

```
amazon_detail/
├── detail.py                           # Core scraping script
├── amazon_products_20250804_131010.csv # Sample output data
├── image.png                           # Result screenshot
└── README.md                           # This file
```

### Overview

A high-performance Amazon product scraper built with Python, featuring advanced anti-bot bypass techniques including **CAPTCHA solving**, **session management**, and **proxy rotation**.

### ✨ Features

- 🔐 **CAPTCHA Solving** - Automatic CAPTCHA recognition using `ddddocr` OCR library
- 🛡️ **Anti-Bot Bypass** - Browser fingerprint simulation with `curl_cffi`
- 🔄 **Session Management** - Automatic session renewal and cookie handling
- 🌐 **Proxy Support** - Configurable proxy rotation for production environments
- ⚡ **Async Processing** - High-performance async scraping
- 📊 **Data Export** - Export to CSV in Shopify-compatible format

### 🔐 Anti-Bot & CAPTCHA Bypass Logic

#### 1. CAPTCHA Detection & Solving

```python
def is_captcha_page(response_text):
    """Detect CAPTCHA page by checking for specific indicators"""
    captcha_indicators = [
        "Enter the characters you see below",
        "Type the characters you see in this image",
        "Sorry, we just need to make sure you're not a robot"
    ]
    return any(indicator in response_text for indicator in captcha_indicators)
```

**CAPTCHA Solving Flow:**
1. Detect CAPTCHA page by checking response text for indicators
2. Extract CAPTCHA image URL from HTML response
3. Download and process image using `ddddocr` OCR library
4. Submit OCR result via Amazon's `validateCaptcha` endpoint
5. Retry with fresh session if solving fails

#### 2. Session Management & Browser Fingerprinting

The scraper uses `curl_cffi` to impersonate real browsers:

- **Browser Impersonation**: Randomly selects from Chrome, Edge, Safari fingerprints
- **Cookie Management**: Generates realistic session cookies (`csm_sid`, `csm-hit`, `ubid-main`)
- **CSRF Token Handling**: Extracts and uses `anti-csrftoken-a2z` for authenticated requests
- **Location Setting**: Sets delivery location to US (zipcode 10001) for consistent pricing

```python
# Browser fingerprint rotation
impersonate = ["chrome99", "chrome120", "edge101", "safari17_0", ...]
session = AsyncSession()
resp = await session.get(url, impersonate=random.choice(impersonate))
```

#### 3. Proxy Configuration

```python
# Local development
proxies = {
    "http": "http://127.0.0.1:7890",
    "https": "http://127.0.0.1:7890",
}

# Production (via environment variables)
proxies = {
    "http": os.getenv("PROXY_URL"),
    "https": os.getenv("PROXY_URL"),
}
```

### 📦 Installation

```bash
pip install ddddocr curl_cffi scrapy parsel aiohttp pandas lxml beautifulsoup4 colorlog
```

### 🚀 Usage

```bash
python detail.py
```

### 📊 Sample Output

The scraper exports data in **Shopify-compatible CSV format**:

![Sample Output](image.png)

**CSV Columns Include:**
| Column | Description |
|--------|-------------|
| Handle | Product URL handle |
| Title | Product title |
| Body (HTML) | Product description |
| Variant SKU | Amazon ASIN |
| Variant Price | Product price |
| Image Src | Product image URL |
| Option1/2/3 Name & Value | Size, Color, etc. |

---

## 中文

### 📁 项目结构

```
amazon_detail/
├── detail.py                           # 核心爬虫脚本
├── amazon_products_20250804_131010.csv # 示例输出数据
├── image.png                           # 结果截图
└── README.md                           # 本文件
```

### 项目简介

一个高性能的亚马逊商品爬虫，采用Python开发，具备完整的**反风控能力**，包括**验证码自动识别**、**会话管理**和**代理轮换**。

### ✨ 功能特点

- 🔐 **验证码识别** - 使用 `ddddocr` OCR库自动识别验证码
- 🛡️ **反风控绑过** - 使用 `curl_cffi` 模拟真实浏览器指纹
- 🔄 **会话管理** - 自动续期会话和Cookie处理
- 🌐 **代理支持** - 支持生产环境代理轮换配置
- ⚡ **异步处理** - 高性能异步爬取
- 📊 **数据导出** - 导出Shopify兼容的CSV格式

### 🔐 反风控与验证码绑过逻辑

#### 1. 验证码检测与识别

```python
def is_captcha_page(response_text):
    """通过检查特定标识符检测验证码页面"""
    captcha_indicators = [
        "Enter the characters you see below",
        "Type the characters you see in this image",
        "Sorry, we just need to make sure you're not a robot"
    ]
    return any(indicator in response_text for indicator in captcha_indicators)
```

**验证码处理流程：**
1. 检测响应文本中的验证码标识符
2. 从HTML响应中提取验证码图片URL
3. 使用 `ddddocr` OCR库识别图片内容
4. 通过亚马逊的 `validateCaptcha` 接口提交识别结果
5. 如果识别失败，使用新会话重试

#### 2. 会话管理与浏览器指纹模拟

爬虫使用 `curl_cffi` 模拟真实浏览器：

- **浏览器指纹轮换**: 随机选择Chrome、Edge、Safari等浏览器指纹
- **Cookie管理**: 生成真实的会话Cookie (`csm_sid`, `csm-hit`, `ubid-main`)
- **CSRF Token处理**: 提取并使用 `anti-csrftoken-a2z` 进行认证请求
- **地区设置**: 设置配送地址为美国(邮编10001)以获取一致的价格

#### 3. 代理配置

```python
# 本地开发
proxies = {
    "http": "http://127.0.0.1:7890",
    "https": "http://127.0.0.1:7890",
}

# 生产环境 (通过环境变量)
proxies = {
    "http": os.getenv("PROXY_URL"),
    "https": os.getenv("PROXY_URL"),
}
```

### 📦 安装依赖

```bash
pip install ddddocr curl_cffi scrapy parsel aiohttp pandas lxml beautifulsoup4 colorlog
```

### 🚀 使用方法

```bash
python detail.py
```

### 📊 输出示例

爬虫导出**Shopify兼容的CSV格式**数据：

![输出示例](image.png)

**CSV字段说明：**
| 字段 | 描述 |
|------|------|
| Handle | 商品URL句柄 |
| Title | 商品标题 |
| Body (HTML) | 商品描述 |
| Variant SKU | 亚马逊ASIN |
| Variant Price | 商品价格 |
| Image Src | 商品图片URL |
| Option1/2/3 Name & Value | 尺寸、颜色等 |

### 📄 License

MIT License