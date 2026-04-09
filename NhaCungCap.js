/**
 * NhaCungCap.js — Tách từ Code.js
 * Dùng chung global scope với Code.js
 */

var TAB_NGUON = 'Nguồn';


// ============================================================
// SYNC NCC — So sánh + append NCC mới vào DanhMuc_NCC
// ============================================================

function _syncNCC(crmIds, nccGDRows, nccMap) {
  var ss = _openCrm_(crmIds, 'NHA_CUNG_CAP');
  var sheet = ss.getSheetByName('DanhMuc_NCC');
  if (!sheet) throw new Error('Không tìm thấy tab DanhMuc_NCC');

  // Đọc ma_ncc hiện có (cột A) + tên nhóm (cột D) để tránh tạo trùng
  var existing = {};
  var data = sheet.getDataRange().getValues();
  for (var i = 1; i < data.length; i++) {
    var maNcc = (data[i][0] || '').toString().trim();
    if (maNcc) existing[maNcc] = true;
    // Đọc ten_nhom (cột D, index 3) → merge vào nccMap để dedup theo tên
    var tenNhom = (data[i][3] || '').toString().trim();
    if (maNcc && tenNhom) {
      var nhomKey = tenNhom.toLowerCase();
      if (!nccMap[nhomKey]) nccMap[nhomKey] = maNcc;
    }
  }

  // Phát hiện NCC mới từ GD NCC
  var newRows = [];
  var maxNum = 0;
  Object.keys(existing).forEach(function(k) {
    var m = k.match(/^NCC-(\d+)$/);
    if (m) { var n = parseInt(m[1]); if (n > maxNum) maxNum = n; }
  });

  nccGDRows.forEach(function(r) {
    if (!r.ten_nguon) return;
    var key = r.ten_nguon.trim().toLowerCase();
    if (nccMap[key]) return; // đã match
    // Tạo NCC mới
    maxNum++;
    var newMa = 'NCC-' + ('000' + maxNum).slice(-3);
    nccMap[key] = newMa;
    existing[newMa] = true;
    newRows.push([
      newMa, r.ten_nguon, '', r.ten_nguon, '', '', '', '', 0, 0, 0, 0, 0,
      'Hoat_dong', false, 'AUTO-DETECTED', '', new Date()
    ]);
    Logger.log('NEW NCC: "' + r.ten_nguon + '" → ' + newMa);
  });

  if (newRows.length > 0) {
    var lastRow = sheet.getLastRow();
    sheet.getRange(lastRow + 1, 1, newRows.length, newRows[0].length).setValues(newRows);
  }

  Logger.log('SyncNCC: ' + newRows.length + ' NCC mới');
  return { added: newRows.length };
}


// ============================================================
// SYNC GD NCC — So sánh + append GD NCC mới vào GD_NhaCungCap
// ============================================================

function _syncGDNCC(crmIds, nccGDRows, nccMap) {
  var ss = _openCrm_(crmIds, 'GD_NCC_' + NAM);
  var sheet = ss.getSheetByName('GD_NhaCungCap');
  if (!sheet) throw new Error('Không tìm thấy tab GD_NhaCungCap');

  // Đọc GD NCC hiện có → composite key + khởi tạo counter
  var existing = {};
  var data = sheet.getDataRange().getValues();
  for (var i = 1; i < data.length; i++) {
    var key = _gdNCCKey(data[i][2], data[i][1], data[i][5], data[i][3]);
    existing[key] = true;
  }
  _initCountersFromExisting_(sheet, 'GD-NCC');

  // Sort theo ngày để sinh mã đúng thứ tự
  nccGDRows.sort(function(a, b) {
    var da = a.ngay instanceof Date ? a.ngay.getTime() : 0;
    var db = b.ngay instanceof Date ? b.ngay.getTime() : 0;
    return da - db;
  });

  var newRows = [];
  var newMaNCCs = {}; // track NCC bị ảnh hưởng để recompute

  nccGDRows.forEach(function(r) {
    var maNcc = _lookupNcc(nccMap, r.ten_nguon);
    var key = _gdNCCKey(maNcc, r.ngay, r.so_tien, r.loai_gd);
    if (existing[key]) return;
    existing[key] = true;

    if (maNcc) newMaNCCs[maNcc] = true;

    var maGd = _generateMaGD_('GD-NCC', r.ngay);

    var doiSoat = _needsDoiSoat(r.loai_gd, r.httt);
    newRows.push([
      maGd,             // ma_gd
      r.ngay,           // ngay_gd
      maNcc,            // ma_ncc
      r.loai_gd,        // loai_gd
      r.httt,           // hinh_thuc_tt
      r.so_tien,        // so_tien_goc
      r.phi,            // phi
      r.tong_chuyen,    // tong_chuyen
      '',               // quy_truoc
      '',               // bien_dong
      '',               // quy_sau
      '',               // ma_gd_kh
      '',               // gom_rate
      '',               // tong_nhan
      'Hoan_thanh',     // trang_thai
      r.nguoi_th,       // nguoi_thuc_hien
      r.ghi_chu,        // ghi_chu
      r.ngay,           // ngay_tao
      '',               // trang_thai_doi_soat (ghi vào DoiSoat_GD thay vì đây)
      '',               // nguoi_doi_soat
      ''                // ngay_doi_soat
    ]);
  });

  if (newRows.length > 0) {
    var lastRow = sheet.getLastRow();
    sheet.getRange(lastRow + 1, 1, newRows.length, newRows[0].length).setValues(newRows);

    // Ghi DoiSoat_GD cho NCC (giống KH)
    var dsRows = [];
    newRows.forEach(function(row) {
      var gdLoai = row[3];  // loai_gd
      var gdHttt = row[4];  // hinh_thuc_tt
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
        Logger.log('DoiSoat_GD (NCC): ' + dsRows.length + ' record mới');
      }
    }
  }

  Logger.log('SyncGDNCC: ' + newRows.length + ' GD NCC mới');
  return { added: newRows.length, newMaNCCs: Object.keys(newMaNCCs) };
}


/**
 * Composite key cho GD NCC (dedup)
 */
function _gdNCCKey(maNcc, ngay, soTien, loaiGD) {
  var d = '';
  if (ngay instanceof Date && !isNaN(ngay.getTime())) {
    d = Utilities.formatDate(ngay, TZ, 'yyyyMMdd');
  }
  return [maNcc || '', d, Math.round((soTien || 0) * 100), loaiGD || ''].join('|');
}


// ============================================================
// RECOMPUTE QUỸ NCC — Tính lại quỹ cho NCC có GD mới
// ============================================================

/**
 * Logic dòng tiền NCC (ngược KH):
 * - Nap_quy:  NCC nhận tiền → Quỹ NCC +
 * - Rut_CID:  Rút tiền từ CID → Quỹ NCC +
 * - Mua_TK:   Trả tiền mua TK → Quỹ NCC -
 * - Nap_CID:  Nạp tiền vào CID → Quỹ NCC -
 * - Refund:   NCC trả lại tiền → Quỹ NCC - (rút sạch)
 */
function _recomputeQuyNCC(crmIds, affectedMaNCCs, quyGocNCCMap) {
  if (!affectedMaNCCs || affectedMaNCCs.length === 0) return;
  Logger.log('Recompute quỹ NCC cho ' + affectedMaNCCs.length + ' NCC: ' + affectedMaNCCs.join(', '));

  var ssGD = _openCrm_(crmIds, 'GD_NCC_' + NAM);
  var sheetGD = ssGD.getSheetByName('GD_NhaCungCap');
  var gdData = sheetGD.getDataRange().getValues();

  var affected = {};
  affectedMaNCCs.forEach(function(mn) { affected[mn] = true; });

  var gdByNCC = {};
  for (var i = 1; i < gdData.length; i++) {
    var mn = (gdData[i][2] || '').toString().trim();
    if (!affected[mn]) continue;
    if (!gdByNCC[mn]) gdByNCC[mn] = [];
    gdByNCC[mn].push({
      rowIndex: i,
      ngay: gdData[i][1],
      loai_gd: (gdData[i][3] || '').toString().trim(),
      so_tien_goc: parseFloat(gdData[i][5]) || 0
    });
  }

  var quyResults = {};
  var gdUpdates = [];

  Object.keys(gdByNCC).forEach(function(mn) {
    var gds = gdByNCC[mn];
    gds.sort(function(a, b) {
      var da = a.ngay instanceof Date ? a.ngay.getTime() : 0;
      var db = b.ngay instanceof Date ? b.ngay.getTime() : 0;
      return da - db;
    });

    var quy = (quyGocNCCMap && quyGocNCCMap[mn]) ? quyGocNCCMap[mn] : 0; // bắt đầu từ quỹ gốc NCC
    gds.forEach(function(gd) {
      var quyTruoc = quy;
      var bienDong = 0;

      if (gd.loai_gd === 'Nap_quy')      bienDong = gd.so_tien_goc;
      else if (gd.loai_gd === 'Rut_CID')  bienDong = gd.so_tien_goc;
      else if (gd.loai_gd === 'Mua_TK')   bienDong = -gd.so_tien_goc;
      else if (gd.loai_gd === 'Nap_CID')  bienDong = -gd.so_tien_goc;
      else if (gd.loai_gd === 'Refund')   bienDong = -quyTruoc; // rút sạch

      quy = quyTruoc + bienDong;
      gdUpdates.push({ rowIndex: gd.rowIndex, quyTruoc: quyTruoc, bienDong: bienDong, quySau: quy });
    });

    quyResults[mn] = quy;
  });

  // Batch write GD NCC: cột I(9), J(10), K(11)
  gdUpdates.forEach(function(u) {
    sheetGD.getRange(u.rowIndex + 1, 9, 1, 3).setValues([[u.quyTruoc, u.bienDong, u.quySau]]);
  });

  // Cập nhật quy_hien_tai trong DanhMuc_NCC
  var ssNCC = _openCrm_(crmIds, 'NHA_CUNG_CAP');
  var sheetNCC = ssNCC.getSheetByName('DanhMuc_NCC');
  var nccData = sheetNCC.getDataRange().getValues();
  var now = new Date();

  for (var j = 1; j < nccData.length; j++) {
    var mn2 = (nccData[j][0] || '').toString().trim();
    if (quyResults[mn2] !== undefined) {
      sheetNCC.getRange(j + 1, 12).setValue(quyResults[mn2]); // cột L = quy_hien_tai
      sheetNCC.getRange(j + 1, 18).setValue(now); // cột R = ngay_cap_nhat
    }
  }

  Logger.log('Recompute NCC done. Updated: ' + Object.keys(quyResults).length);
}


/**
 * Tương tự cho NCC
 */
function _createKickOffNCC(crmIds, nccMap) {
  var source = SpreadsheetApp.openById(SOURCE_ID);
  var sheetNguon = source.getSheetByName(TAB_NGUON);
  if (!sheetNguon) return;
  var nguonData = sheetNguon.getDataRange().getValues();
  var headerRow = nguonData[0];
  var colKickOff = _findDateCol(headerRow, KICKOFF_DATE);
  if (colKickOff < 0) {
    Logger.log('WARNING: Không tìm thấy cột ' + Utilities.formatDate(KICKOFF_DATE, TZ, 'dd/MM/yyyy') + ' trong Nguồn');
    return;
  }

  var ktNCCMap = {};
  for (var i = 3; i < nguonData.length; i++) {
    var tenNguon = (nguonData[i][1] || '').toString().trim();
    if (!tenNguon) continue;
    var maNcc = _lookupNcc(nccMap, tenNguon);
    if (!maNcc) continue;
    ktNCCMap[maNcc] = parseFloat(nguonData[i][colKickOff]) || 0;
  }

  // Đọc quỹ gốc NCC + GD NCC từ CRM
  var ssNCC = _openCrm_(crmIds, 'NHA_CUNG_CAP');
  var sheetNCC = ssNCC.getSheetByName('DanhMuc_NCC');
  var nccData = sheetNCC.getDataRange().getValues();

  var ssGD = _openCrm_(crmIds, 'GD_NCC_' + NAM);
  var sheetGD = ssGD.getSheetByName('GD_NhaCungCap');
  var gdData = sheetGD ? sheetGD.getDataRange().getValues() : [];
  _initCountersFromExisting_(sheetGD, 'GD-NCC');

  // Đọc quỹ gốc NCC
  var nccQuyGocImport = _importQuyGocNCC(crmIds, nccMap) || {};

  // Tính quỹ CRM NCC CHỈ ĐẾN KICKOFF_DATE
  var kickoffEnd = new Date(KICKOFF_DATE.getFullYear(), KICKOFF_DATE.getMonth(), KICKOFF_DATE.getDate(), 23, 59, 59);
  var crmNCCAtKickoff = {};
  for (var mn in nccQuyGocImport) crmNCCAtKickoff[mn] = nccQuyGocImport[mn];
  // Cũng init NCC có GD nhưng không có quỹ gốc
  for (var g = 1; g < gdData.length; g++) {
    var gMN = (gdData[g][2] || '').toString().trim();
    if (gMN && crmNCCAtKickoff[gMN] === undefined) crmNCCAtKickoff[gMN] = 0;
  }

  // Tính tuần tự (Refund cần quỹ hiện tại)
  for (var mn2 in crmNCCAtKickoff) {
    var gds = [];
    for (var g2 = 1; g2 < gdData.length; g2++) {
      if ((gdData[g2][2]||'').toString().trim() !== mn2) continue;
      var gN = gdData[g2][1];
      if (!(gN instanceof Date) || gN.getTime() > kickoffEnd.getTime()) continue;
      gds.push({ ngay: gN, loai_gd: (gdData[g2][3]||'').toString().trim(), so_tien: parseFloat(gdData[g2][5])||0 });
    }
    gds.sort(function(a,b) { return (a.ngay?a.ngay.getTime():0) - (b.ngay?b.ngay.getTime():0); });
    var quy = crmNCCAtKickoff[mn2];
    gds.forEach(function(gd) {
      if (gd.loai_gd === 'Nap_quy') quy += gd.so_tien;
      else if (gd.loai_gd === 'Rut_CID') quy += gd.so_tien;
      else if (gd.loai_gd === 'Mua_TK') quy -= gd.so_tien;
      else if (gd.loai_gd === 'Nap_CID') quy -= gd.so_tien;
      else if (gd.loai_gd === 'Refund') quy = 0;
    });
    crmNCCAtKickoff[mn2] = quy;
  }



  var newRows = [];
  var dsRows = [];
  for (var j = 1; j < nccData.length; j++) {
    var maNcc = (nccData[j][0] || '').toString().trim();
    if (!maNcc || ktNCCMap[maNcc] === undefined) continue;
    var quyCRM = crmNCCAtKickoff[maNcc] || 0; // quỹ CRM đến 31/03
    var quyKT = ktNCCMap[maNcc];
    var diff = quyKT - quyCRM;
    if (Math.abs(diff) < 0.01) continue;

    var maGd = _generateMaGD_('GD-NCC', KICKOFF_DATE);
    // NCC: dùng Nap_quy (+) hoặc Mua_TK (-), KHÔNG dùng Refund (rút sạch)
    var loai = diff > 0 ? 'Nap_quy' : 'Mua_TK';
    var soTien = Math.abs(diff);

    newRows.push([
      maGd, KICKOFF_DATE, maNcc, loai, '', soTien, 0, soTien,
      '', '', '', '', '', '', 'Hoan_thanh', 'Migration', 'Kick-Off CRM',
      KICKOFF_DATE, '', '', ''
    ]);
    // Ghi đối soát vào DoiSoat_GD (không ghi trực tiếp vào GD_NhaCungCap)
    dsRows.push([maGd, 'Da_doi_soat']);
  }

  if (newRows.length > 0) {
    var lastRow = sheetGD.getLastRow();
    sheetGD.getRange(lastRow + 1, 1, newRows.length, newRows[0].length).setValues(newRows);

    var ssDoi = _openCrm_(crmIds, 'DOI_SOAT_' + NAM);
    var sheetDoi = ssDoi.getSheetByName('DoiSoat_GD');
    if (sheetDoi && dsRows.length > 0) {
      var lastDoi = sheetDoi.getLastRow();
      sheetDoi.getRange(lastDoi + 1, 1, dsRows.length, dsRows[0].length).setValues(dsRows);
    }
  }

  Logger.log('Kick-Off NCC: ' + newRows.length + ' GD cân bằng tạo ngày ' + Utilities.formatDate(KICKOFF_DATE, TZ, 'dd/MM/yyyy'));
  return newRows.length;
}


// ============================================================
// ĐỌC DỮ LIỆU ĐỐI CHIẾU — PHẦN NCC
// ============================================================

function _readDoiChieuNCC() {
  var source = SpreadsheetApp.openById(SOURCE_ID);
  var sheet = source.getSheetByName(TAB_DOI_CHIEU);
  if (!sheet) return [];

  var data = sheet.getDataRange().getValues();
  if (data.length <= 1) return [];

  var headers = data[0];
  var col = {};
  headers.forEach(function(h, i) { col[h.toString().trim()] = i; });

  // Debug: log tên tất cả cột để tìm đúng tên cột NCC
  if (col['Giao dịch với Nguồn'] === undefined) {
    Logger.log('WARNING: Không tìm thấy cột "Giao dịch với Nguồn". Các cột hiện có:');
    headers.forEach(function(h, i) {
      var name = h.toString().trim();
      if (name) Logger.log('  Col ' + i + ': "' + name + '"');
    });
    // Thử tìm cột gần đúng
    headers.forEach(function(h, i) {
      var name = h.toString().trim().toLowerCase();
      if (name.indexOf('nguồn') >= 0 || name.indexOf('nguon') >= 0) {
        Logger.log('  → Có thể là cột NCC: Col ' + i + ': "' + h.toString().trim() + '"');
      }
    });
  }

  var rows = [];
  for (var i = 1; i < data.length; i++) {
    var r = data[i];

    var ngay = _parseDate(r[col['Ngày']]);
    if (!ngay || ngay < DATE_FROM) continue;

    var gdNguon = (r[col['Giao dịch với Nguồn']] || '').toString().trim();
    if (!gdNguon) continue;
    var gdNguonLower = gdNguon.toLowerCase();

    var loaiGD = '';
    if (gdNguonLower === 'nạp tiền cho nguồn') {
      loaiGD = 'Nap_quy';
    } else if (gdNguonLower === 'mua tài khoản') {
      loaiGD = 'Mua_TK';
    } else {
      continue;
    }

    var htttRaw = (r[col['Hình thức thanh toán']] || '').toString().trim();
    if (htttRaw.toLowerCase().indexOf('ghim') >= 0) continue;

    var httt = '';
    if (htttRaw === 'Trừ quỹ') {
      httt = 'tru_quy';
    } else if (htttRaw === 'Chuyển-Nhận') {
      httt = 'truc_tiep';
    }

    var tenNguon = (r[col['Tên Zalo/Telegram']] || '').toString().trim();

    var phiRaw = (r[col['Phần trăm phí nạp (%)']] || '').toString().trim();
    var phi = 0;
    if (phiRaw) {
      var phiMatch = phiRaw.toString().match(/[\d.]+/);
      phi = phiMatch ? parseFloat(phiMatch[0]) : 0;
    }

    var soTienGoc = _parseNumber(r[col['Số tiền nạp (chưa tính % phí)']]);
    var tongChuyen = _parseNumber(r[col['Số tiền (Tổng)']]);
    var soLuongTK = parseInt(r[col['Số lượng TK']]) || 0;
    var nguoiTH = (r[col['Người thực hiện']] || '').toString().trim();
    var ghiChu = (r[col['Ghi chú']] || '').toString().trim();
    var cidRaw = (r[col['ID Tài khoản ( Nếu nạp quỹ thì ghi - )']] || '').toString().trim();

    if (loaiGD === 'Nap_quy') {
      rows.push({
        ngay: ngay, ten_nguon: tenNguon, loai_gd: loaiGD, httt: httt,
        cid: '', so_tien: soTienGoc, phi: phi, tong_chuyen: tongChuyen,
        nguoi_th: nguoiTH, ghi_chu: ghiChu, dong: i + 1
      });
    } else if (loaiGD === 'Mua_TK') {
      var cids = _parseCIDs(cidRaw, i + 1, 'Đối chiếu NCC');
      var divisor = soLuongTK > 1 ? soLuongTK : 1;
      var soTienPerCID = soTienGoc / divisor;
      var tongPerCID = tongChuyen / divisor;

      if (cids.length === 0) {
        rows.push({
          ngay: ngay, ten_nguon: tenNguon, loai_gd: loaiGD, httt: httt,
          cid: '', so_tien: soTienGoc, phi: phi, tong_chuyen: tongChuyen,
          nguoi_th: nguoiTH, ghi_chu: ghiChu, dong: i + 1
        });
      } else {
        for (var c = 0; c < cids.length; c++) {
          rows.push({
            ngay: ngay, ten_nguon: tenNguon, loai_gd: loaiGD, httt: httt,
            cid: cids[c], so_tien: soTienPerCID, phi: phi, tong_chuyen: tongPerCID,
            nguoi_th: nguoiTH, ghi_chu: ghiChu, dong: i + 1
          });
        }
      }
    }
  }

  Logger.log('Đối chiếu NCC rows sau lọc: ' + rows.length);
  return rows;
}


/**
 * Import quỹ gốc NCC từ tab "Nguồn" cột HZ (31/12/2025)
 * Ghi vào DanhMuc_NCC cột quy_goc (nếu có)
 */
function _importQuyGocNCC(crmIds, nccMap) {
  var source = SpreadsheetApp.openById(SOURCE_ID);
  var sheet = source.getSheetByName(TAB_NGUON);
  if (!sheet) {
    Logger.log('WARNING: Không tìm thấy tab "' + TAB_NGUON + '" để import quỹ gốc NCC');
    return;
  }

  var data = sheet.getDataRange().getValues();
  if (data.length <= 3) return;

  var colTenNguon = 1; // B
  // Tab Nguồn: dòng 1 (index 0) chứa ngày
  var colQuyGocNCC = _findDateCol(data[0], QUY_GOC_DATE);
  if (colQuyGocNCC < 0) {
    Logger.log('WARNING: Không tìm thấy cột ngày ' + Utilities.formatDate(QUY_GOC_DATE, TZ, 'dd/MM/yyyy') + ' trong tab Nguồn');
    return {};
  }

  // Đọc quỹ gốc theo tên NCC → map sang mã NCC
  var quyGocMap = {}; // { ma_ncc: quy_goc }
  for (var i = 3; i < data.length; i++) {
    var tenNguon = (data[i][colTenNguon] || '').toString().trim();
    if (!tenNguon) continue;
    var maNcc = _lookupNcc(nccMap, tenNguon);
    if (!maNcc) continue;
    var quyGoc = parseFloat(data[i][colQuyGocNCC]) || 0;
    quyGocMap[maNcc] = quyGoc;
  }

  if (Object.keys(quyGocMap).length === 0) {
    Logger.log('Import quỹ gốc NCC: không có NCC nào để import');
    return;
  }

  // Ghi vào DanhMuc_NCC — tìm cột quy_goc (nếu chưa có thì dùng cột khác)
  // DanhMuc_NCC hiện tại: cột L (12) = quy_hien_tai
  // Cần thêm cột quy_goc — dùng cột phụ hoặc tính trực tiếp trong recompute
  // Approach: không ghi cột riêng, thay vào đó truyền quyGocMap vào _recomputeQuyNCC
  Logger.log('Import quỹ gốc NCC: đọc ' + Object.keys(quyGocMap).length + ' NCC');
  return quyGocMap;
}


function _doiChieuQuyNCC(crmIds, nccMap) {
  var source = SpreadsheetApp.openById(SOURCE_ID);
  var sheet = source.getSheetByName(TAB_NGUON);
  if (!sheet) { Logger.log('WARNING: Không tìm thấy tab Nguồn'); return; }

  var data = sheet.getDataRange().getValues();
  if (data.length <= 3) return;

  var headerRow = data[0];
  var colTenNguon = 1;

  // Map NCC row index + mã NCC
  var nccRows = {}; // { ma_ncc: rowIdx }
  for (var i = 3; i < data.length; i++) {
    var tenNguon = (data[i][colTenNguon] || '').toString().trim();
    if (!tenNguon) continue;
    var maNcc = _lookupNcc(nccMap, tenNguon);
    if (maNcc) nccRows[maNcc] = i;
  }

  // Đọc quỹ gốc NCC
  var colQuyGocNCC = _findDateCol(headerRow, QUY_GOC_DATE);
  var nccQuyGocMap = {};
  if (colQuyGocNCC >= 0) {
    for (var mn in nccRows) {
      nccQuyGocMap[mn] = parseFloat(data[nccRows[mn]][colQuyGocNCC]) || 0;
    }
  }

  // Tìm cột ngày từ DOICHIEU_FROM → cuối
  var dateCols = [];
  for (var c = 3; c < headerRow.length; c++) {
    var val = headerRow[c];
    var d = null;
    if (val instanceof Date && !isNaN(val.getTime())) d = val;
    else if (val) d = _parseDate(val.toString().trim());
    if (d && d.getTime() >= DOICHIEU_FROM.getTime()) {
      dateCols.push({ col: c, date: d, dateStr: Utilities.formatDate(d, TZ, 'dd/MM/yyyy') });
    }
  }

  if (dateCols.length === 0) { Logger.log('Đối chiếu NCC: không có cột ngày >= DOICHIEU_FROM'); return; }

  // Đọc GD NCC từ CRM
  var ssGD = _openCrm_(crmIds, 'GD_NCC_' + NAM);
  var sheetGD = ssGD.getSheetByName('GD_NhaCungCap');
  var gdData = sheetGD ? sheetGD.getDataRange().getValues() : [];

  // Đối chiếu từng ngày
  var allWarnings = [];
  var logRows = [];
  var now = new Date();

  for (var dc = 0; dc < dateCols.length; dc++) {
    var dateInfo = dateCols[dc];
    var cutoff = new Date(dateInfo.date.getFullYear(), dateInfo.date.getMonth(), dateInfo.date.getDate(), 23, 59, 59);

    // Tính quỹ CRM NCC đến ngày này (tuần tự vì Refund)
    for (var mn2 in nccRows) {
      var quy = nccQuyGocMap[mn2] || 0;
      // Collect + sort GD cho NCC này đến ngày
      var gds = [];
      for (var g = 1; g < gdData.length; g++) {
        var gdMN = (gdData[g][2] || '').toString().trim();
        if (gdMN !== mn2) continue;
        var gdNgay = gdData[g][1];
        if (!(gdNgay instanceof Date) || gdNgay.getTime() > cutoff.getTime()) continue;
        gds.push({ ngay: gdNgay, loai_gd: (gdData[g][3] || '').toString().trim(), so_tien: parseFloat(gdData[g][5]) || 0 });
      }
      gds.sort(function(a, b) { return (a.ngay ? a.ngay.getTime() : 0) - (b.ngay ? b.ngay.getTime() : 0); });
      gds.forEach(function(gd) {
        if (gd.loai_gd === 'Nap_quy') quy += gd.so_tien;
        else if (gd.loai_gd === 'Rut_CID') quy += gd.so_tien;
        else if (gd.loai_gd === 'Mua_TK') quy -= gd.so_tien;
        else if (gd.loai_gd === 'Nap_CID') quy -= gd.so_tien;
        else if (gd.loai_gd === 'Refund') quy = 0;
      });

      var quyKT = parseFloat(data[nccRows[mn2]][dateInfo.col]) || 0;
      var diff = quy - quyKT;
      if (Math.abs(diff) > 0.01) {
        allWarnings.push({ ma_ncc: mn2, ngay: dateInfo.dateStr, quy_crm: quy, quy_kt: quyKT, lech: diff });
        logRows.push([now, mn2, quy, quyKT, diff, dateInfo.dateStr]);
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

  // ── Telegram NCC: Mỗi NCC = verify 31/03 + ngày đầu lệch từ 01/04 ──
  var kickoffDateStr = Utilities.formatDate(KICKOFF_DATE, TZ, 'dd/MM/yyyy');
  var nccWarn31 = {};
  var nccAfter01 = {};
  allWarnings.forEach(function(w) {
    if (w.ngay === kickoffDateStr) {
      nccWarn31[w.ma_ncc] = w;
    } else if (!nccAfter01[w.ma_ncc]) {
      nccAfter01[w.ma_ncc] = w;
    }
  });

  var showNCC = {};
  for (var n1 in nccWarn31) showNCC[n1] = true;
  for (var n2 in nccAfter01) showNCC[n2] = true;

  var msg = '📊 *Đối chiếu quỹ NCC* (' + dateCols[0].dateStr + ' → ' + dateCols[dateCols.length-1].dateStr + ')\n\n';
  var nccLech31 = Object.keys(nccWarn31).length;
  var nccLechAfter = Object.keys(nccAfter01).length;

  if (Object.keys(showNCC).length > 0) {
    Object.keys(showNCC).sort().forEach(function(mn) {
      msg += '• `' + mn + '`:\n';
      var w31 = nccWarn31[mn];
      if (w31) {
        msg += '  31/03: CRM $' + w31.quy_crm.toFixed(2) + ' vs KT $' + w31.quy_kt.toFixed(2) + ' ❌ lệch $' + w31.lech.toFixed(2) + '\n';
      } else {
        msg += '  31/03: ✅ khớp\n';
      }
      var wAfter = nccAfter01[mn];
      if (wAfter) {
        msg += '  ' + wAfter.ngay + ': CRM $' + wAfter.quy_crm.toFixed(2) + ' vs KT $' + wAfter.quy_kt.toFixed(2) + ' (lệch $' + wAfter.lech.toFixed(2) + ')\n';
      } else {
        msg += '  Từ 01/04: ✅ khớp\n';
      }
    });
  } else {
    msg += '✅ Tất cả ' + Object.keys(nccRows).length + ' NCC khớp (31/03 + 01/04→cuối)\n';
  }

  msg += '\nTổng: ' + nccLech31 + ' NCC lệch 31/03, ' + nccLechAfter + ' NCC lệch từ 01/04';
  _sendTelegram(msg);
  Logger.log('Đối chiếu NCC: ' + nccLech31 + ' lệch 31/03, ' + nccLechAfter + ' lệch từ 01/04');
}


// ============================================================
// ĐỌC CONFIG MAPPING NGUỒN → MA_NCC
// ============================================================

/**
 * Đọc mapping tên nguồn → mã NCC từ tab "Config_NccMap" trong file NHA_CUNG_CAP
 * Tab gồm 2 cột: ten_nguon | ma_ncc
 * Trả về map { ten_nguon_lower: ma_ncc }
 */
function _readNccMap() {
  var crmIds = _loadCrmIds_();
  var ss = _openCrm_(crmIds, 'NHA_CUNG_CAP');
  var sheet = ss.getSheetByName('Config_NccMap');
  var map = {};
  if (!sheet) {
    Logger.log('WARNING: Không tìm thấy tab "Config_NccMap" trong NHA_CUNG_CAP — dùng ten_nhom từ DanhMuc_NCC để dedup');
    return map;
  }

  var data = sheet.getDataRange().getValues();
  for (var i = 1; i < data.length; i++) {
    var tenNguon = (data[i][0] || '').toString().trim();
    var maNcc = (data[i][1] || '').toString().trim();
    if (tenNguon && maNcc) {
      map[tenNguon.toLowerCase()] = maNcc;
    }
  }
  Logger.log('NccMap loaded: ' + Object.keys(map).length + ' mappings');
  return map;
}


function _lookupNcc(nccMap, nguon) {
  if (!nguon) return '';
  return nccMap[nguon.trim().toLowerCase()] || '';
}
