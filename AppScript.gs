/**
 * QA Signal Agent — Google Apps Script Web App
 *
 * SETUP INSTRUCTIONS:
 * 1. Open your Google Sheet
 * 2. Extensions → Apps Script
 * 3. Delete any existing code and paste this entire file
 * 4. Click Save (Ctrl+S)
 * 5. Click "Deploy" → "New deployment"
 * 6. Type: Web app
 * 7. Execute as: Me
 * 8. Who has access: Anyone
 * 9. Click Deploy → Authorise → Copy the Web App URL
 * 10. Add to .env:  GOOGLE_SHEET_URL=https://script.google.com/...
 * 11. Add to GitHub Secrets as GOOGLE_SHEET_URL
 *
 * The script will auto-create a "QA Signals" sheet with headers
 * and append one row per company per day.
 */

var SHEET_NAME = "QA Signals";

var HEADERS = [
  "Date",
  "Company",
  "Score",
  "# Roles",
  "Positions",
  "Location",
  "Industry",
  "Employees",
  "Funding Stage",
  "Founded",
  "Funding News",
  "Product News",
  "Tech Stack",
  "AI Signal",
  "Leadership Hiring",
  "Repeat Hiring",
  "Hiring Velocity",
  "Leadership Open (Live)",
  "Contacts",
];


// ── Helpers ────────────────────────────────────────────────────────────────────

function getOrCreateSheet() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(SHEET_NAME);

  if (!sheet) {
    sheet = ss.insertSheet(SHEET_NAME);

    // Write headers
    sheet.getRange(1, 1, 1, HEADERS.length).setValues([HEADERS]);

    // Style header row
    var headerRange = sheet.getRange(1, 1, 1, HEADERS.length);
    headerRange.setBackground("#2c3e50");
    headerRange.setFontColor("#ffffff");
    headerRange.setFontWeight("bold");
    headerRange.setFontSize(11);

    // Freeze header row
    sheet.setFrozenRows(1);

    // Set column widths
    sheet.setColumnWidth(1, 100);  // Date
    sheet.setColumnWidth(2, 180);  // Company
    sheet.setColumnWidth(3, 60);   // Score
    sheet.setColumnWidth(4, 60);   // # Roles
    sheet.setColumnWidth(5, 250);  // Positions
    sheet.setColumnWidth(6, 160);  // Location
    sheet.setColumnWidth(7, 160);  // Industry
    sheet.setColumnWidth(8, 90);   // Employees
    sheet.setColumnWidth(9, 110);  // Funding Stage
    sheet.setColumnWidth(10, 80);  // Founded
    sheet.setColumnWidth(11, 250); // Funding News
    sheet.setColumnWidth(12, 250); // Product News
    sheet.setColumnWidth(13, 200); // Tech Stack
    sheet.setColumnWidth(14, 300); // AI Signal
    sheet.setColumnWidth(15, 200); // Leadership
    sheet.setColumnWidth(16, 180); // Repeat Hiring
    sheet.setColumnWidth(17, 200); // Hiring Velocity
    sheet.setColumnWidth(18, 220); // Leadership Open (Live)
    sheet.setColumnWidth(19, 280); // Contacts
  }

  return sheet;
}


function rowColor(score) {
  if (score >= 9)  return "#fde8e8";   // high — light red
  if (score >= 7)  return "#fef3e2";   // medium-high — light orange
  if (score >= 5)  return "#e8f4fd";   // medium — light blue
  return "#ffffff";                     // low — white
}


// ── Main handler ───────────────────────────────────────────────────────────────

function doPost(e) {
  try {
    var payload = JSON.parse(e.postData.contents);
    var rows = payload.rows || [];

    if (rows.length === 0) {
      return ContentService
        .createTextOutput(JSON.stringify({ status: "ok", written: 0 }))
        .setMimeType(ContentService.MimeType.JSON);
    }

    var sheet = getOrCreateSheet();
    var written = 0;

    for (var i = 0; i < rows.length; i++) {
      var r = rows[i];

      var rowData = [
        r.date          || "",
        r.company       || "",
        r.score         || 0,
        r.num_roles     || 0,
        r.positions     || "",
        r.location      || "",
        r.industry      || "",
        r.employees     || "",
        r.funding_stage || "",
        r.founded_year  || "",
        r.funding_news  || "",
        r.product_news  || "",
        r.tech_stack    || "",
        r.ai_signal     || "",
        r.leadership          || "",
        r.repeat_hiring       || "",
        r.hiring_velocity     || "",
        r.linkedin_leadership || "",
        r.contacts            || "",
      ];

      var lastRow = sheet.getLastRow() + 1;
      sheet.getRange(lastRow, 1, 1, HEADERS.length).setValues([rowData]);

      // Colour-code by score
      sheet.getRange(lastRow, 1, 1, HEADERS.length)
           .setBackground(rowColor(r.score || 0));

      written++;
    }

    // Auto-resize columns after writing (optional, can remove if slow)
    // sheet.autoResizeColumns(1, HEADERS.length);

    return ContentService
      .createTextOutput(JSON.stringify({ status: "ok", written: written }))
      .setMimeType(ContentService.MimeType.JSON);

  } catch (err) {
    return ContentService
      .createTextOutput(JSON.stringify({ status: "error", message: err.toString() }))
      .setMimeType(ContentService.MimeType.JSON);
  }
}


// ── Test function (run manually from Apps Script editor to verify) ─────────────

function testSheet() {
  var sheet = getOrCreateSheet();
  var testRow = [
    "2026-04-17", "Test Company", 9, 3,
    "QA Engineer, SDET, Test Lead",
    "Bangalore, India",
    "Information Technology",
    1200, "Series B", 2018,
    "Test Company raises $50M Series B",
    "Test Company launches AI testing platform",
    "Selenium, Cypress, AWS",
    "We are building AI-first quality platform",
    "",
    "Hired QA 3 times in last 6 months",
    "47+ open roles on LinkedIn (rapidly scaling)",
    "Hiring: VP Engineering, Head of Product",
    "John Doe | QA Manager | john@testcompany.com"
  ];

  var lastRow = sheet.getLastRow() + 1;
  sheet.getRange(lastRow, 1, 1, HEADERS.length).setValues([testRow]);
  sheet.getRange(lastRow, 1, 1, HEADERS.length).setBackground(rowColor(9));

  Logger.log("Test row written to row " + lastRow);
}
