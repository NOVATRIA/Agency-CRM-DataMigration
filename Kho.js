/**
 * Kho.js — Tách từ Code.js
 * Dùng chung global scope với Code.js
 */

var _TRANG_THAI_MAP = {
  'chưa bán': 'Chua_ban',
  'chua ban':  'Chua_ban',
  'đã bán':   'Da_ban',
  'da ban':    'Da_ban',
  'huỷ':       'Huy',
  'hủy':       'Huy',
  'huy':       'Huy',
  'bảo hành':  'Bao_hanh',
  'bao hanh':  'Bao_hanh'
};

var START_ROW_KHO = 2347; // Chỉ quét từ dòng này trở đi (tháng 01/2026)


function _syncKho(crmIds, allRows, nccMap, khoData) {
  var ss = _openCrm_(crmIds, 'KHO_TK');
  var sheet = ss.getSheetByName('Kho_TaiKhoan');
  if (!sheet) throw new Error('Không tìm thấy tab Kho_TaiKhoan');

  var CID_REGEX = /^\d{3}-\d{3}-\d{4}$/;

  // Đọc CID hiện có (cột A)
  var existing = {};
  var data = sheet.getDataRange().getValues();
  for (var i = 1; i < data.length; i++) {
    var cid = (data[i][0] || '').toString().trim();
    if (cid) existing[cid] = true;
  }

  var newRows = [];

  // ── Nguồn 1: CID từ "TK trong kho" (chỉ từ 01/01/2026) ──
  khoData.forEach(function(tk) {
    if (existing[tk.cid]) return; // đã có trong CRM
    // Chỉ lấy CID nhập từ 01/01/2026
    if (tk.ngay_nhap && tk.ngay_nhap instanceof Date && tk.ngay_nhap < DATE_FROM) return;

    var maNcc = _lookupNcc(nccMap, tk.ten_group);
    if (!maNcc && tk.ten_group) {
      _errors.push({
        tab: 'TK trong kho', dong: tk.dong,
        loai_loi: 'Thiếu Tên Group', gia_tri: tk.ten_group,
        ghi_chu: 'Không tìm được mã NCC cho Tên Group này'
      });
    }

    var trangThai = _mapTrangThai(tk.tinh_trang, tk.ma_kh);
    if (trangThai === null) {
      _errors.push({
        tab: 'TK trong kho', dong: tk.dong,
        loai_loi: 'Tình trạng lạ', gia_tri: tk.tinh_trang,
        ghi_chu: 'Không nằm trong danh sách map: Chưa bán / Huỷ / Bảo hành'
      });
      trangThai = tk.ma_kh ? 'Da_ban' : 'Chua_ban';
    }

    existing[tk.cid] = true;
    newRows.push([
      tk.cid,           // cid
      maNcc,            // ma_ncc
      tk.ma_kh || '',   // ma_kh
      trangThai,        // trang_thai
      tk.ngay_nhap || '',  // ngay_nhap
      tk.ngay_ban || '',   // ngay_ban
      '',               // ghi_chu
      new Date()        // ngay_cap_nhat
    ]);
  });

  // ── Nguồn 2: CID từ GD (Topup + Đối chiếu) chưa có trong "TK trong kho" ──
  allRows.forEach(function(r) {
    if (!r.cid || r.cid === '-') return;
    if (!MA_KH_REGEX.test(r.ma_kh)) return;
    if (!CID_REGEX.test(r.cid)) return;
    if (existing[r.cid]) return;

    var maNcc = _lookupNcc(nccMap, r.nhom_nguon || '');
    existing[r.cid] = true;
    newRows.push([
      r.cid,          // cid
      maNcc,          // ma_ncc
      r.ma_kh,        // ma_kh
      'Da_ban',       // trang_thai (có GD → đã bán)
      r.ngay,         // ngay_nhap
      r.ngay,         // ngay_ban
      '',             // ghi_chu
      new Date()      // ngay_cap_nhat
    ]);
  });

  if (newRows.length > 0) {
    var lastRow = sheet.getLastRow();
    sheet.getRange(lastRow + 1, 1, newRows.length, newRows[0].length).setValues(newRows);
  }

  Logger.log('SyncKho: ' + newRows.length + ' CID mới (đã có trước: ' + (Object.keys(existing).length - newRows.length) + ')');
  return { added: newRows.length, existing: Object.keys(existing).length - newRows.length };
}


function _readTKTrongKho() {
  var source = SpreadsheetApp.openById(SOURCE_ID);
  var sheet = source.getSheetByName(TAB_TK_KHO);
  if (!sheet) {
    Logger.log('WARNING: Không tìm thấy tab "' + TAB_TK_KHO + '"');
    return { list: [], groupMap: {} };
  }

  var data = sheet.getDataRange().getValues();
  if (data.length <= 7) return { list: [], groupMap: {} };

  var headers = data[6];
  var col = {};
  headers.forEach(function(h, i) { col[h.toString().trim()] = i; });

  var colCID = col['CID'];
  var colGroup = col['Tên Group'];
  if (colCID === undefined || colGroup === undefined) {
    Logger.log('WARNING: Tab "' + TAB_TK_KHO + '" thiếu cột CID hoặc Tên Group');
    return { list: [], groupMap: {} };
  }

  var colNgayNhap = col['Ngày nhập TK'];
  var colTinhTrang = col['Tình trạng'];
  var colMaKH = col['Mã KH'];
  var colNgayCap = col['Ngày cấp'];

  var CID_REGEX = /^\d{3}-\d{3}-\d{4}$/;
  var list = [];
  var groupMap = {};

  var startIdx = Math.max(7, START_ROW_KHO - 1); // START_ROW_KHO là 1-based, index là 0-based
  for (var i = startIdx; i < data.length; i++) {
    var row = data[i];
    var rawCid = row[colCID];
    var group = (row[colGroup] || '').toString().trim();

    // Bỏ qua dòng trống hoàn toàn
    if (!rawCid && !group) continue;

    var cid = _formatCID(rawCid);

    // Validation: CID trống
    if (!cid) {
      if (group) {
        _errors.push({
          tab: 'TK trong kho', dong: i + 1,
          loai_loi: 'CID trống', gia_tri: '',
          ghi_chu: 'Dòng có Tên Group "' + group + '" nhưng CID trống'
        });
      }
      continue;
    }

    // Validation: CID sai format
    if (!CID_REGEX.test(cid)) {
      _errors.push({
        tab: 'TK trong kho', dong: i + 1,
        loai_loi: 'CID sai format', gia_tri: cid,
        ghi_chu: 'Không đúng format XXX-XXX-XXXX'
      });
      continue;
    }

    var tinh_trang = (colTinhTrang !== undefined) ? (row[colTinhTrang] || '').toString().trim() : '';
    var ma_kh = (colMaKH !== undefined) ? (row[colMaKH] || '').toString().trim() : '';
    var ngay_nhap = (colNgayNhap !== undefined) ? _parseDate(row[colNgayNhap]) : null;
    var ngay_ban = (colNgayCap !== undefined) ? _parseDate(row[colNgayCap]) : null;

    // Auto-correct Mã KH
    ma_kh = _fixMaKH(ma_kh);

    // Validation: Mã KH sai format
    if (ma_kh && !MA_KH_REGEX.test(ma_kh)) {
      _errors.push({
        tab: 'TK trong kho', dong: i + 1,
        loai_loi: 'Mã KH sai format', gia_tri: ma_kh,
        ghi_chu: 'CID ' + cid + ' — Mã KH không đúng format LLK-XXXXXX'
      });
      ma_kh = ''; // bỏ qua Mã KH sai
    }

    list.push({
      cid: cid,
      ten_group: group,
      tinh_trang: tinh_trang,
      ma_kh: ma_kh,
      ngay_nhap: ngay_nhap,
      ngay_ban: ngay_ban,
      dong: i + 1
    });

    if (group) groupMap[cid] = group;
  }

  Logger.log('TK trong kho: ' + list.length + ' CID đọc được');
  return { list: list, groupMap: groupMap };
}


function _mapTrangThai(raw, maKH) {
  if (!raw) return maKH ? 'Da_ban' : 'Chua_ban';
  var key = raw.toString().trim().toLowerCase();
  var mapped = _TRANG_THAI_MAP[key];
  if (mapped) return mapped;
  // Giá trị lạ → log lỗi, mặc định dựa vào Mã KH
  return null;
}
