# AGENTS.md — Agency-CRM-DataMigration

Script Data Migration cho LLK Agency.

---

## Dự án này là gì

- **App:** `DataMigration`
- **Mục tiêu:** Đồng bộ dữ liệu từ nguồn cũ vào CRM DB theo cơ chế dedup và ghi log.

---

## Cấu trúc thư mục chính

```text
Agency-CRM-DataMigration/
├── AGENTS.md
├── README.md
├── .Codex/
│   └── commands/
│       ├── prime.md
│       ├── create-plan.md
│       └── implement.md
├── context/
├── plans/
├── docs/
└── (source files)
```

---

## Deploy

```bash
clasp push
clasp deploy
```

---

## Quy tắc làm việc

1. Đọc file trước khi sửa.
2. Tài liệu phải đúng phạm vi repo hiện tại.
3. Khi đổi cấu trúc, cập nhật `AGENTS.md` và `context/current-data.md`.
4. Việc lớn cần tạo plan trong `plans/` trước khi triển khai.
