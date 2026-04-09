/**
 * KhachHang.js — Tách từ Code.js
 * Dùng chung global scope với Code.js
 */

var TAB_TONG_HOP = 'Tổng hợp';


// ============================================================
// SYNC KH — So sánh + append KH mới vào DanhMuc_KH
// ============================================================

function _syncKH(crmIds, allRows) {
  var ss = _openCrm_(crmIds, 'KHACH_HANG');
  var sheet = ss.getSheetByName('DanhMuc_KH');
  if (!sheet) throw new Error('Không tìm thấy tab DanhMuc_KH');

  // Đọc ma_kh hiện có (cột A)
  var existing = {};
  var data = sheet.getDataRange().getValues();
  for (var i = 1; i < data.length; i++) {
    var mk = (data[i][0] || '').toString().trim();
    if (mk) existing[mk] = true;
  }

  // Tìm KH mới từ source
  var khMap = {};
  allRows.forEach(function(r) {
    if (!MA_KH_REGEX.test(r.ma_kh)) return;
    if (existing[r.ma_kh]) return; // đã có trong CRM
    if (!khMap[r.ma_kh]) {
      khMap[r.ma_kh] = { ten_kh: r.ten_kh || '', ngay_dau: r.ngay };
    } else if (r.ngay < khMap[r.ma_kh].ngay_dau) {
      khMap[r.ma_kh].ngay_dau = r.ngay;
    }
  });

  // Append KH mới
  var newRows = [];
  var maKHs = Object.keys(khMap).sort();
  maKHs.forEach(function(mk) {
    var info = khMap[mk];
    newRows.push([
      mk,              // ma_kh
      info.ten_kh,     // ten_kh
      '',              // email
      0,               // quy_hien_tai (recompute tính)
      0,               // quy_goc
      0,               // so_cid
      'Hoat_dong',     // trang_thai
      '',              // ghi_chu
      info.ngay_dau,   // ngay_tao
      new Date()       // ngay_cap_nhat
    ]);
  });

  if (newRows.length > 0) {
    var lastRow = sheet.getLastRow();
    sheet.getRange(lastRow + 1, 1, newRows.length, newRows[0].length).setValues(newRows);
  }

  Logger.log('SyncKH: ' + newRows.length + ' KH mới (đã có: ' + Object.keys(existing).length + ')');
  return { added: newRows.length, existing: Object.keys(existing).length };
}


// ============================================================
// SYNC GD KH — So sánh + append GD KH mới vào GD_KhachHang
// ============================================================

function _syncGDKH(crmIds, allRows, nccMap) {
  var ss = _openCrm_(crmIds, 'GD_KH_' + NAM);
  var sheet = ss.getSheetByName('GD_KhachHang');
  if (!sheet) throw new Error('Không tìm thấy tab GD_KhachHang');

  // Đọc GD hiện có → tạo Set composite key + khởi tạo counter
  var existing = {};
  var data = sheet.getDataRange().getValues();
  for (var i = 1; i < data.length; i++) {
    var key = _gdKHKey(data[i][1], data[i][14], data[i][4], data[i][5], data[i][2]);
    existing[key] = true;
  }
  _initCountersFromExisting_(sheet, 'GD-KH');

  // Sort allRows theo ngày để sinh mã GD đúng thứ tự
  allRows.sort(function(a, b) {
    var da = a.ngay instanceof Date ? a.ngay.getTime() : 0;
    var db = b.ngay instanceof Date ? b.ngay.getTime() : 0;
    return da - db;
  });

  // Tìm GD mới
  var newRows = [];
  var newMaKHs = {}; // track KH bị ảnh hưởng để recompute

  allRows.forEach(function(r) {
    if (!MA_KH_REGEX.test(r.ma_kh)) return;

    var loaiGD, httt, soTien, phi, tongKHChuyen, cid;

    if (r.source === 'doichieu') {
      loaiGD = r.loai_gd;
      httt = r.httt;
      soTien = r.so_tien;
      phi = r.phi;
      tongKHChuyen = r.tong_kh_chuyen;
      cid = r.cid;
    } else {
      // Topup
      loaiGD = r.yeu_cau === 'DEPOSIT' ? 'Nap_CID' : 'Rut_CID';
      httt = '';
      soTien = r.so_tien;
      phi = 0;
      tongKHChuyen = r.so_tien;
      cid = r.cid;
    }

    var key = _gdKHKey(r.ma_kh, r.ngay, cid, soTien, loaiGD);
    if (existing[key]) return; // đã có

    existing[key] = true; // tránh trùng trong batch hiện tại
    newMaKHs[r.ma_kh] = true;

    var maGd = _generateMaGD_('GD-KH', r.ngay);

    newRows.push([
      maGd,             // ma_gd
      r.ma_kh,          // ma_kh
      loaiGD,           // loai_gd
      httt,             // hinh_thuc_tt
      cid,              // cid
      soTien,           // so_tien_goc
      phi,              // phi
      tongKHChuyen,     // tong_kh_chuyen
      '',               // quy_truoc (recompute)
      '',               // bien_dong (recompute)
      '',               // quy_sau (recompute)
      'Hoan_thanh',     // trang_thai
      r.nguoi_th,       // nguoi_thuc_hien
      r.ghi_chu || '',  // ghi_chu
      r.ngay,           // ngay_tao
      r.ngay            // ngay_thuc_hien
    ]);
  });

  if (newRows.length > 0) {
    var lastRow = sheet.getLastRow();
    sheet.getRange(lastRow + 1, 1, newRows.length, newRows[0].length).setValues(newRows);

    // Tạo record đối soát cho GD cần đối soát (trạng thái Da_doi_soat vì là dữ liệu lịch sử)
    var dsRows = [];
    newRows.forEach(function(row) {
      var gdLoai = row[2];  // loai_gd
      var gdHttt = row[3];  // hinh_thuc_tt
      if (_needsDoiSoat(gdLoai, gdHttt)) {
        dsRows.push([row[0], 'Da_doi_soat']); // [ma_gd, trang_thai_doi_soat]
      }
    });
    if (dsRows.length > 0) {
      var ssDoi = _openCrm_(crmIds, 'DOI_SOAT_' + NAM);
      var sheetDoi = ssDoi.getSheetByName('DoiSoat_GD');
      if (sheetDoi) {
        var lastDoi = sheetDoi.getLastRow();
        sheetDoi.getRange(lastDoi + 1, 1, dsRows.length, dsRows[0].length).setValues(dsRows);
        Logger.log('DoiSoat_GD: ' + dsRows.length + ' record mới');
      }
    }
  }

  Logger.log('SyncGDKH: ' + newRows.length + ' GD mới (đã có: ' + Object.keys(existing).length + ')');
  return { added: newRows.length, newMaKHs: Object.keys(newMaKHs) };
}


/**
 * Kiểm tra GD có cần đối soát không (logic khớp CRM Helpers.js:needsDoiSoat)
 */
function _needsDoiSoat(loaiGD, httt) {
  if (loaiGD === 'Nap_quy') return true;
  if (loaiGD === 'Refund')  return true;
  if ((loaiGD === 'Mua_TK' || loaiGD === 'Nap_CID') && httt === 'truc_tiep') return true;
  return false;
}


/**
 * Composite key cho GD KH (dedup)
 * ma_kh + ngay(yyyyMMdd) + cid + so_tien + loai_gd
 */
function _gdKHKey(maKH, ngay, cid, soTien, loaiGD) {
  var d = '';
  if (ngay instanceof Date && !isNaN(ngay.getTime())) {
    d = Utilities.formatDate(ngay, TZ, 'yyyyMMdd');
  }
  return [maKH || '', d, cid || '', Math.round((soTien || 0) * 100), loaiGD || ''].join('|');
}


// ============================================================
// RECOMPUTE QUỸ KH — Tính lại quỹ cho KH có GD mới
// ============================================================

/**
 * Tính biến động quỹ KH — logic khớp CRM Helpers.js:calculateBienDong()
 */
function _calculateBienDongKH(loaiGD, httt, soTienGoc) {
  if (loaiGD === 'Nap_quy')   return soTienGoc;
  if (loaiGD === 'Rut_CID')   return soTienGoc;
  if (loaiGD === 'Cashback')  return soTienGoc;
  if (loaiGD === 'Refund')    return -soTienGoc;
  if (loaiGD === 'Nap_CID' || loaiGD === 'Mua_TK') {
    return (httt === 'truc_tiep') ? 0 : -soTienGoc;
  }
  return 0;
}


function _recomputeQuyKH(crmIds, affectedMaKHs) {
  if (!affectedMaKHs || affectedMaKHs.length === 0) return;
  Logger.log('Recompute quỹ cho ' + affectedMaKHs.length + ' KH: ' + affectedMaKHs.join(', '));

  // Đọc tất cả GD KH
  var ssGD = _openCrm_(crmIds, 'GD_KH_' + NAM);
  var sheetGD = ssGD.getSheetByName('GD_KhachHang');
  var gdData = sheetGD.getDataRange().getValues();

  // Đọc quỹ gốc từ DanhMuc_KH
  var ssKH = _openCrm_(crmIds, 'KHACH_HANG');
  var sheetKH = ssKH.getSheetByName('DanhMuc_KH');
  var khData = sheetKH.getDataRange().getValues();
  var quyGocMap = {}; // { ma_kh: quy_goc }
  for (var k = 1; k < khData.length; k++) {
    var mkk = (khData[k][0] || '').toString().trim();
    if (mkk) quyGocMap[mkk] = parseFloat(khData[k][4]) || 0; // cột E = quy_goc
  }

  // Group GD theo ma_kh, chỉ lấy affected
  var affected = {};
  affectedMaKHs.forEach(function(mk) { affected[mk] = true; });

  var gdByKH = {};
  for (var i = 1; i < gdData.length; i++) {
    var mk = (gdData[i][1] || '').toString().trim();
    if (!affected[mk]) continue;
    if (!gdByKH[mk]) gdByKH[mk] = [];
    gdByKH[mk].push({
      rowIndex: i,
      ngay: gdData[i][14], // ngay_tao
      loai_gd: (gdData[i][2] || '').toString().trim(),
      httt: (gdData[i][3] || '').toString().trim(),
      so_tien_goc: parseFloat(gdData[i][5]) || 0
    });
  }

  // Tính quỹ cho từng KH — batch update
  var quyResults = {};
  var gdUpdates = []; // { rowIndex, quyTruoc, bienDong, quySau }

  Object.keys(gdByKH).forEach(function(mk) {
    var gds = gdByKH[mk];
    gds.sort(function(a, b) {
      var da = a.ngay instanceof Date ? a.ngay.getTime() : 0;
      var db = b.ngay instanceof Date ? b.ngay.getTime() : 0;
      return da - db;
    });

    var quy = quyGocMap[mk] || 0; // bắt đầu từ quỹ gốc
    gds.forEach(function(gd) {
      var quyTruoc = quy;
      var bienDong = _calculateBienDongKH(gd.loai_gd, gd.httt, gd.so_tien_goc);
      quy = quyTruoc + bienDong;

      gdUpdates.push({
        rowIndex: gd.rowIndex,
        quyTruoc: quyTruoc,
        bienDong: bienDong,
        quySau: quy
      });
    });

    quyResults[mk] = quy;
  });

  // Batch write GD: cập nhật cột I(9), J(10), K(11)
  gdUpdates.forEach(function(u) {
    var rowNum = u.rowIndex + 1;
    sheetGD.getRange(rowNum, 9, 1, 3).setValues([[u.quyTruoc, u.bienDong, u.quySau]]);
  });

  // Batch write DanhMuc_KH: cập nhật quy_hien_tai + ngay_cap_nhat
  var now = new Date();
  for (var j = 1; j < khData.length; j++) {
    var mk2 = (khData[j][0] || '').toString().trim();
    if (quyResults[mk2] !== undefined) {
      sheetKH.getRange(j + 1, 4, 1, 1).setValue(quyResults[mk2]); // cột D = quy_hien_tai
      sheetKH.getRange(j + 1, 10, 1, 1).setValue(now); // cột J = ngay_cap_nhat
    }
  }

  Logger.log('Recompute KH done. Updated: ' + Object.keys(quyResults).length);
}


// ============================================================
// KICK-OFF CRM — Tạo GD cân bằng ngày 31/03 cho KH chênh lệch
// ============================================================

/**
 * So sánh quỹ CRM (sau recompute) với cột KICKOFF_DATE trong "Tổng hợp".
 * Nếu chênh lệch → tạo GD "Kick_Off" ngày KICKOFF_DATE để cân bằng.
 */
function _createKickOffKH(crmIds) {
  // Đọc quỹ KT tại KICKOFF_DATE
  var source = SpreadsheetApp.openById(SOURCE_ID);
  var sheetTH = source.getSheetByName(TAB_TONG_HOP);
  if (!sheetTH) return;
  var thData = sheetTH.getDataRange().getValues();
  var headerRow = thData[1];
  var colKickOff = _findDateCol(headerRow, KICKOFF_DATE);
  if (colKickOff < 0) {
    Logger.log('WARNING: Không tìm thấy cột ' + Utilities.formatDate(KICKOFF_DATE, TZ, 'dd/MM/yyyy') + ' trong Tổng hợp');
    return;
  }

  var ktMap = {}; // { ma_kh: quỹ KT tại KICKOFF_DATE }
  for (var i = 5; i < thData.length; i++) {
    var mk = _fixMaKH((thData[i][3] || '').toString().trim());
    if (!mk || !MA_KH_REGEX.test(mk)) continue;
    ktMap[mk] = parseFloat(thData[i][colKickOff]) || 0;
  }

  // Đọc quỹ gốc + GD KH từ CRM
  var ssKH = _openCrm_(crmIds, 'KHACH_HANG');
  var sheetKH = ssKH.getSheetByName('DanhMuc_KH');
  var khData = sheetKH.getDataRange().getValues();

  var quyGocMap = {};
  for (var kg = 1; kg < khData.length; kg++) {
    var mkg = (khData[kg][0] || '').toString().trim();
    if (mkg) quyGocMap[mkg] = parseFloat(khData[kg][4]) || 0; // cột E = quy_goc
  }

  var ssGD = _openCrm_(crmIds, 'GD_KH_' + NAM);
  var sheetGD = ssGD.getSheetByName('GD_KhachHang');
  var gdData = sheetGD ? sheetGD.getDataRange().getValues() : [];
  _initCountersFromExisting_(sheetGD, 'GD-KH');

  // Tính quỹ CRM CHỈ ĐẾN KICKOFF_DATE (không tính GD sau 31/03)
  var kickoffEnd = new Date(KICKOFF_DATE.getFullYear(), KICKOFF_DATE.getMonth(), KICKOFF_DATE.getDate(), 23, 59, 59);
  var crmAtKickoff = {}; // { ma_kh: quỹ CRM đến 31/03 }
  for (var mk2 in quyGocMap) crmAtKickoff[mk2] = quyGocMap[mk2];
  for (var g = 1; g < gdData.length; g++) {
    var gMK = (gdData[g][1] || '').toString().trim();
    if (!gMK || crmAtKickoff[gMK] === undefined) continue;
    var gNgay = gdData[g][14]; // ngay_tao
    if (!(gNgay instanceof Date) || gNgay.getTime() > kickoffEnd.getTime()) continue;
    crmAtKickoff[gMK] += _calculateBienDongKH(
      (gdData[g][2]||'').toString().trim(),
      (gdData[g][3]||'').toString().trim(),
      parseFloat(gdData[g][5]) || 0
    );
  }

  // Tìm KH chênh lệch → tạo GD Kick-Off
  var newRows = [];
  var dsRows = [];
  for (var j = 1; j < khData.length; j++) {
    var maKH = (khData[j][0] || '').toString().trim();
    if (!maKH || ktMap[maKH] === undefined) continue;
    var quyCRM = crmAtKickoff[maKH] || 0; // quỹ CRM đến 31/03
    var quyKT = ktMap[maKH];
    var diff = quyKT - quyCRM;
    if (Math.abs(diff) < 0.01) continue; // khớp rồi

    var maGd = _generateMaGD_('GD-KH', KICKOFF_DATE);
    var loai = diff > 0 ? 'Nap_quy' : 'Refund';
    var soTien = Math.abs(diff);

    newRows.push([
      maGd, maKH, loai, '', '', soTien, 0, soTien,
      '', '', '', 'Hoan_thanh', 'Migration', 'Kick-Off CRM',
      KICKOFF_DATE, KICKOFF_DATE
    ]);
    dsRows.push([maGd, 'Da_doi_soat']);
  }

  if (newRows.length > 0) {
    var lastRow = sheetGD.getLastRow();
    sheetGD.getRange(lastRow + 1, 1, newRows.length, newRows[0].length).setValues(newRows);

    // Ghi DoiSoat
    var ssDoi = _openCrm_(crmIds, 'DOI_SOAT_' + NAM);
    var sheetDoi = ssDoi.getSheetByName('DoiSoat_GD');
    if (sheetDoi && dsRows.length > 0) {
      var lastDoi = sheetDoi.getLastRow();
      sheetDoi.getRange(lastDoi + 1, 1, dsRows.length, dsRows[0].length).setValues(dsRows);
    }
  }

  Logger.log('Kick-Off KH: ' + newRows.length + ' GD cân bằng tạo ngày ' + Utilities.formatDate(KICKOFF_DATE, TZ, 'dd/MM/yyyy'));
  return newRows.length;
}


// ============================================================
// ĐỌC DỮ LIỆU TOPUP
// ============================================================

function _readTopup() {
  var source = SpreadsheetApp.openById(SOURCE_ID);
  var sheet = source.getSheetByName(TAB_TOPUP);
  if (!sheet) throw new Error('Không tìm thấy tab "' + TAB_TOPUP + '" trong sheet nguồn');

  var data = sheet.getDataRange().getValues();
  if (data.length <= 1) return [];

  var headers = data[0];
  var col = {};
  headers.forEach(function(h, i) { col[h.toString().trim()] = i; });

  var needed = ['Thời gian', 'Mã khách hàng', 'ID Tài Khoản', 'Số tiền',
                'Loại tiền', 'Yêu cầu', 'Nguồn', 'Nhóm nguồn',
                'Người thực hiện', 'Tình trạng', 'Note'];
  needed.forEach(function(h) {
    if (col[h] === undefined) throw new Error('Thiếu cột "' + h + '" trong tab Topup');
  });

  var rows = [];
  for (var i = 1; i < data.length; i++) {
    var r = data[i];

    var ngay = _parseDate(r[col['Thời gian']]);
    if (!ngay || ngay < DATE_FROM) continue;

    var maKH = _fixMaKH((r[col['Mã khách hàng']] || '').toString().trim());
    if (!maKH) continue;

    if (NCC_MA_KH.indexOf(maKH) >= 0) continue;

    var cid = _formatCID(r[col['ID Tài Khoản']]);
    var tinhTrang = (r[col['Tình trạng']] || '').toString().trim().toLowerCase();
    if (tinhTrang !== 'done') continue;

    var yeuCau = (r[col['Yêu cầu']] || '').toString().trim().toUpperCase();
    if (yeuCau !== 'DEPOSIT' && yeuCau !== 'WITHDRAW') continue;

    rows.push({
      ngay: ngay,
      ma_kh: maKH,
      cid: cid,
      so_tien: _parseNumber(r[col['Số tiền']]),
      loai_tien: (r[col['Loại tiền']] || 'USD').toString().trim(),
      yeu_cau: yeuCau,
      nguon: (r[col['Nguồn']] || '').toString().trim(),
      nhom_nguon: (r[col['Nhóm nguồn']] || '').toString().trim(),
      nguoi_th: (r[col['Người thực hiện']] || '').toString().trim(),
      ghi_chu: (r[col['Note']] || '').toString().trim()
    });
  }

  return rows;
}


// ============================================================
// ĐỌC DỮ LIỆU ĐỐI CHIẾU TÀI CHÍNH
// ============================================================

function _readDoiChieu() {
  var source = SpreadsheetApp.openById(SOURCE_ID);
  var sheet = source.getSheetByName(TAB_DOI_CHIEU);
  if (!sheet) throw new Error('Không tìm thấy tab "' + TAB_DOI_CHIEU + '" trong sheet nguồn');

  var data = sheet.getDataRange().getValues();
  if (data.length <= 1) return [];

  var headers = data[0];
  var col = {};
  headers.forEach(function(h, i) { col[h.toString().trim()] = i; });

  var needed = ['Ngày', 'Mã KH', 'Tên Zalo/Telegram', 'Tên nguồn (Tài khoản)',
                'Giao dịch với khách', 'Số lượng TK',
                'Số tiền nạp (chưa tính % phí)', 'Phần trăm phí nạp (%)',
                'Số tiền (Tổng)', 'Hình thức thanh toán',
                'Ghi chú', 'ID Tài khoản ( Nếu nạp quỹ thì ghi - )', 'Người thực hiện'];
  needed.forEach(function(h) {
    if (col[h] === undefined) throw new Error('Thiếu cột "' + h + '" trong tab Đối chiếu');
  });

  var rows = [];
  for (var i = 1; i < data.length; i++) {
    var r = data[i];

    var ngay = _parseDate(r[col['Ngày']]);
    if (!ngay || ngay < DATE_FROM) continue;

    var maKH = _fixMaKH((r[col['Mã KH']] || '').toString().trim());
    if (!maKH) continue;

    if (NCC_MA_KH.indexOf(maKH) >= 0) continue;

    var gdKhach = (r[col['Giao dịch với khách']] || '').toString().trim();
    if (!gdKhach) continue;
    var gdKhachLower = gdKhach.toLowerCase();
    if (gdKhachLower !== 'khách nạp tiền' && gdKhachLower !== 'khách mua tk' && gdKhachLower !== 'refund') continue;

    var htttRaw = (r[col['Hình thức thanh toán']] || '').toString().trim();
    var httt = '';
    if (htttRaw === 'Trừ quỹ') {
      httt = 'tru_quy';
    } else if (htttRaw === 'Chuyển-Nhận') {
      httt = 'truc_tiep';
    } else if (htttRaw && gdKhachLower === 'khách mua tk') {
      if (htttRaw.toLowerCase().indexOf('ghim') >= 0) continue;
    }

    var phiRaw = (r[col['Phần trăm phí nạp (%)']] || '').toString().trim();
    var phi = 0;
    if (phiRaw) {
      var phiMatch = phiRaw.toString().match(/[\d.]+/);
      phi = phiMatch ? parseFloat(phiMatch[0]) : 0;
    }

    var soTienGoc = _parseNumber(r[col['Số tiền nạp (chưa tính % phí)']]);
    var tongKHChuyen = _parseNumber(r[col['Số tiền (Tổng)']]);
    var soLuongTK = parseInt(r[col['Số lượng TK']]) || 0;
    var nguon = (r[col['Tên nguồn (Tài khoản)']] || '').toString().trim();
    var nguoiTH = (r[col['Người thực hiện']] || '').toString().trim();
    var ghiChu = (r[col['Ghi chú']] || '').toString().trim();
    var cidRaw = (r[col['ID Tài khoản ( Nếu nạp quỹ thì ghi - )']] || '').toString().trim();

    if (gdKhachLower === 'khách nạp tiền') {
      rows.push({
        ngay: ngay, ma_kh: maKH, ten_kh: '', cid: '',
        so_tien: soTienGoc, phi: phi, tong_kh_chuyen: tongKHChuyen,
        loai_gd: 'Nap_quy', httt: '', nguon: nguon,
        nguoi_th: nguoiTH, ghi_chu: ghiChu, source: 'doichieu'
      });
    } else if (gdKhachLower === 'refund') {
      rows.push({
        ngay: ngay, ma_kh: maKH, ten_kh: '', cid: '',
        so_tien: soTienGoc, phi: 0, tong_kh_chuyen: soTienGoc,
        loai_gd: 'Refund', httt: '', nguon: nguon,
        nguoi_th: nguoiTH, ghi_chu: ghiChu, source: 'doichieu'
      });
    } else if (gdKhachLower === 'khách mua tk') {
      var cids = _parseCIDs(cidRaw, i + 1, 'Đối chiếu');
      var divisor = soLuongTK > 1 ? soLuongTK : 1;
      var soTienPerCID = soTienGoc / divisor;
      var tongPerCID = tongKHChuyen / divisor;

      if (cids.length === 0) {
        rows.push({
          ngay: ngay, ma_kh: maKH, ten_kh: '', cid: '',
          so_tien: soTienGoc, phi: phi, tong_kh_chuyen: tongKHChuyen,
          loai_gd: 'Mua_TK', httt: httt, nguon: nguon,
          nguoi_th: nguoiTH, ghi_chu: ghiChu, source: 'doichieu'
        });
      } else {
        for (var c = 0; c < cids.length; c++) {
          rows.push({
            ngay: ngay, ma_kh: maKH, ten_kh: '', cid: cids[c],
            so_tien: soTienPerCID, phi: phi, tong_kh_chuyen: tongPerCID,
            loai_gd: 'Mua_TK', httt: httt, nguon: nguon,
            nguoi_th: nguoiTH, ghi_chu: ghiChu, source: 'doichieu'
          });
        }
      }
    }
  }

  return rows;
}


/**
 * Đọc quỹ gốc từ tab "Tổng hợp" — cột tương ứng QUY_GOC_DATE
 * Trả về { ma_kh: quy_goc }
 */
function _readQuyGoc() {
  var source = SpreadsheetApp.openById(SOURCE_ID);
  var sheet = source.getSheetByName(TAB_TONG_HOP);
  if (!sheet) {
    Logger.log('WARNING: Không tìm thấy tab "' + TAB_TONG_HOP + '"');
    return {};
  }

  var data = sheet.getDataRange().getValues();
  if (data.length <= 5) return {};

  var colMaKH = 3; // D
  var headerRow = data[1]; // dòng 2 chứa ngày

  // Tìm cột quỹ gốc theo QUY_GOC_DATE
  var colQuyGoc = _findDateCol(headerRow, QUY_GOC_DATE);
  if (colQuyGoc < 0) {
    Logger.log('WARNING: Không tìm thấy cột ngày ' + Utilities.formatDate(QUY_GOC_DATE, TZ, 'dd/MM/yyyy') + ' trong Tổng hợp');
    return {};
  }
  Logger.log('Quỹ gốc: cột ' + colQuyGoc + ' = ' + Utilities.formatDate(QUY_GOC_DATE, TZ, 'dd/MM/yyyy'));

  // Đọc tất cả KH có mã hợp lệ
  var map = {};
  for (var i = 5; i < data.length; i++) {
    var maKH = _fixMaKH((data[i][colMaKH] || '').toString().trim());
    if (!maKH || !MA_KH_REGEX.test(maKH)) continue;
    var quyGoc = parseFloat(data[i][colQuyGoc]) || 0;
    map[maKH] = quyGoc;
  }

  Logger.log('Quỹ gốc: đọc ' + Object.keys(map).length + ' KH từ cột ' + colQuyGoc + ' (' + Utilities.formatDate(QUY_GOC_DATE, TZ, 'dd/MM/yyyy') + ')');
  return map;
}


/**
 * Import quỹ gốc vào DanhMuc_KH (cột E = quy_goc)
 */
function _importQuyGoc(crmIds) {
  var quyGocMap = _readQuyGoc();
  if (Object.keys(quyGocMap).length === 0) return;

  var ss = _openCrm_(crmIds, 'KHACH_HANG');
  var sheet = ss.getSheetByName('DanhMuc_KH');
  var data = sheet.getDataRange().getValues();
  var updated = 0;

  for (var i = 1; i < data.length; i++) {
    var maKH = (data[i][0] || '').toString().trim();
    if (quyGocMap[maKH] !== undefined) {
      sheet.getRange(i + 1, 5).setValue(quyGocMap[maKH]); // cột E = quy_goc
      updated++;
    }
  }

  Logger.log('Import quỹ gốc KH: ' + updated + ' KH cập nhật');
}


/**
 * Đối chiếu quỹ CRM vs quỹ Kế Toán (cột ngày mới nhất trong "Tổng hợp")
 * Ghi Warning_Log + gửi Telegram nếu có chênh lệch
 */
function _doiChieuQuy(crmIds) {
  var source = SpreadsheetApp.openById(SOURCE_ID);
  var sheet = source.getSheetByName(TAB_TONG_HOP);
  if (!sheet) return;

  var data = sheet.getDataRange().getValues();
  if (data.length <= 5) return;

  var headerRow = data[1];
  var colMaKH = 3;

  // Đọc quỹ CRM hiện tại
  var ssKH = _openCrm_(crmIds, 'KHACH_HANG');
  var sheetKH = ssKH.getSheetByName('DanhMuc_KH');
  var khData = sheetKH.getDataRange().getValues();
  var crmMap = {}; // { ma_kh: quy_hien_tai }
  for (var k = 1; k < khData.length; k++) {
    var mkk = (khData[k][0] || '').toString().trim();
    if (mkk) crmMap[mkk] = parseFloat(khData[k][3]) || 0;
  }

  // Đọc GD KH từ CRM (để tính quỹ đến từng ngày)
  var ssGD = _openCrm_(crmIds, 'GD_KH_' + NAM);
  var sheetGD = ssGD.getSheetByName('GD_KhachHang');
  var gdData = sheetGD ? sheetGD.getDataRange().getValues() : [];

  // Đọc quỹ gốc
  var quyGocMap = {};
  for (var kg = 1; kg < khData.length; kg++) {
    var mkg = (khData[kg][0] || '').toString().trim();
    if (mkg) quyGocMap[mkg] = parseFloat(khData[kg][4]) || 0; // cột E
  }

  // Tìm tất cả cột ngày từ DOICHIEU_FROM → cuối
  var dateCols = []; // { col, date, dateStr }
  for (var c = 6; c < headerRow.length; c++) {
    var val = headerRow[c];
    var d = null;
    if (val instanceof Date && !isNaN(val.getTime())) d = val;
    else if (val) d = _parseDate(val.toString().trim());
    if (d && d.getTime() >= DOICHIEU_FROM.getTime()) {
      dateCols.push({ col: c, date: d, dateStr: Utilities.formatDate(d, TZ, 'dd/MM/yyyy') });
    }
  }

  if (dateCols.length === 0) {
    Logger.log('Đối chiếu KH: không có cột ngày >= ' + Utilities.formatDate(DOICHIEU_FROM, TZ, 'dd/MM/yyyy'));
    return;
  }
  Logger.log('Đối chiếu KH: ' + dateCols.length + ' ngày từ ' + dateCols[0].dateStr + ' → ' + dateCols[dateCols.length-1].dateStr);

  // Đọc KH data từ Tổng hợp (chỉ KH có trong CRM)
  var khRows = {}; // { ma_kh: { rowIdx } }
  for (var i = 5; i < data.length; i++) {
    var maKH = _fixMaKH((data[i][colMaKH] || '').toString().trim());
    if (!maKH || !MA_KH_REGEX.test(maKH)) continue;
    if (crmMap[maKH] === undefined) continue; // chỉ KH có trong CRM
    khRows[maKH] = i;
  }

  // Đối chiếu từng ngày: tính quỹ CRM đến ngày đó
  var allWarnings = []; // { ma_kh, ngay, quyCRM, quyKT, lech }
  var logRows = [];
  var now = new Date();

  for (var dc = 0; dc < dateCols.length; dc++) {
    var dateInfo = dateCols[dc];
    var cutoffEnd = new Date(dateInfo.date.getFullYear(), dateInfo.date.getMonth(), dateInfo.date.getDate(), 23, 59, 59);

    // Tính quỹ CRM cho mỗi KH đến ngày này
    var crmAtDate = {}; // { ma_kh: quỹ }
    for (var mk in quyGocMap) {
      crmAtDate[mk] = quyGocMap[mk]; // bắt đầu từ quỹ gốc
    }
    // Cộng GD đến ngày
    for (var g = 1; g < gdData.length; g++) {
      var gdMK = (gdData[g][1] || '').toString().trim();
      if (!gdMK || crmAtDate[gdMK] === undefined) continue;
      var gdNgay = gdData[g][14]; // ngay_tao
      if (!(gdNgay instanceof Date) || gdNgay.getTime() > cutoffEnd.getTime()) continue;
      var gdLoai = (gdData[g][2] || '').toString().trim();
      var gdHttt = (gdData[g][3] || '').toString().trim();
      var gdSoTien = parseFloat(gdData[g][5]) || 0;
      crmAtDate[gdMK] += _calculateBienDongKH(gdLoai, gdHttt, gdSoTien);
    }

    // So sánh với KT
    for (var mk2 in khRows) {
      var rowIdx = khRows[mk2];
      var quyKT = parseFloat(data[rowIdx][dateInfo.col]) || 0;
      var quyCRM = crmAtDate[mk2] || 0;
      var diff = quyCRM - quyKT;
      if (Math.abs(diff) > 0.01) {
        allWarnings.push({ ma_kh: mk2, ngay: dateInfo.dateStr, quy_crm: quyCRM, quy_kt: quyKT, lech: diff });
        logRows.push([now, mk2, quyCRM, quyKT, diff, dateInfo.dateStr]);
      }
    }
  }

  // Ghi Warning_Log
  if (logRows.length > 0) {
    var logSS = _getLogSpreadsheet_();
    var warnSheet = logSS.getSheetByName('Warning_Log');
    if (!warnSheet) {
      warnSheet = logSS.insertSheet('Warning_Log');
      warnSheet.getRange(1, 1, 1, 6).setValues([['Thời gian', 'Mã', 'Quỹ CRM', 'Quỹ KT', 'Chênh lệch', 'Ngày']]);
      warnSheet.getRange(1, 1, 1, 6).setBackground('#FF9800').setFontColor('#FFFFFF').setFontWeight('bold');
      warnSheet.setFrozenRows(1);
    }
    var lr = warnSheet.getLastRow();
    warnSheet.getRange(lr + 1, 1, logRows.length, 6).setValues(logRows);
  }

  // ── Telegram: Mỗi KH = verify 31/03 + ngày đầu lệch từ 01/04 ──
  var kickoffDateStr = Utilities.formatDate(KICKOFF_DATE, TZ, 'dd/MM/yyyy');

  // Lấy kết quả 31/03 từ allWarnings
  var warn31 = {}; // { ma_kh: warning }
  var after01 = {}; // { ma_kh: warning đầu tiên sau 31/03 }
  allWarnings.forEach(function(w) {
    if (w.ngay === kickoffDateStr) {
      warn31[w.ma_kh] = w;
    } else if (!after01[w.ma_kh]) {
      after01[w.ma_kh] = w;
    }
  });

  // Tìm tất cả KH cần hiện (có lệch 31/03 hoặc lệch sau 01/04)
  var showKH = {};
  for (var k1 in warn31) showKH[k1] = true;
  for (var k2 in after01) showKH[k2] = true;

  var msg = '📊 *Đối chiếu quỹ KH* (' + dateCols[0].dateStr + ' → ' + dateCols[dateCols.length-1].dateStr + ')\n\n';
  var khLech31 = Object.keys(warn31).length;
  var khLechAfter = Object.keys(after01).length;

  if (Object.keys(showKH).length > 0) {
    Object.keys(showKH).sort().forEach(function(mk) {
      msg += '• `' + mk + '`:\n';
      // 31/03
      var w31 = warn31[mk];
      if (w31) {
        msg += '  31/03: CRM $' + w31.quy_crm.toFixed(2) + ' vs KT $' + w31.quy_kt.toFixed(2) + ' ❌ lệch $' + w31.lech.toFixed(2) + '\n';
      } else {
        msg += '  31/03: ✅ khớp\n';
      }
      // Từ 01/04
      var wAfter = after01[mk];
      if (wAfter) {
        msg += '  ' + wAfter.ngay + ': CRM $' + wAfter.quy_crm.toFixed(2) + ' vs KT $' + wAfter.quy_kt.toFixed(2) + ' (lệch $' + wAfter.lech.toFixed(2) + ')\n';
      } else {
        msg += '  Từ 01/04: ✅ khớp\n';
      }
    });
  } else {
    msg += '✅ Tất cả ' + Object.keys(khRows).length + ' KH khớp (31/03 + 01/04→cuối)\n';
  }

  msg += '\nTổng: ' + khLech31 + ' KH lệch 31/03, ' + khLechAfter + ' KH lệch từ 01/04';
  _sendTelegram(msg);
  Logger.log('Đối chiếu KH: ' + khLech31 + ' lệch 31/03, ' + khLechAfter + ' lệch từ 01/04');
}
