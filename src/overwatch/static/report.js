const themePicker = document.querySelector(".theme-picker");
const selectedTheme = localStorage.getItem("overwatch-theme") || "system";
function applyTheme(theme) {
  document.documentElement.dataset.theme = theme;
  themePicker.value = theme;
}
applyTheme(selectedTheme);
themePicker.addEventListener("change", () => {
  applyTheme(themePicker.value);
  localStorage.setItem("overwatch-theme", themePicker.value);
});
for (const table of document.querySelectorAll("table")) {
  const headers = [...table.querySelectorAll("thead th")];
  headers.forEach((header, column) => {
    header.tabIndex = 0;
    const sort = () => {
      const ascending = header.getAttribute("aria-sort") !== "ascending";
      headers.forEach(other => other.removeAttribute("aria-sort"));
      header.setAttribute("aria-sort", ascending ? "ascending" : "descending");

      const rows = [...table.tBodies[0].rows].map((row, index) => ({ row, index }));
      rows.sort((left, right) => {
        const leftValue = left.row.cells[column].dataset.sortValue ?? left.row.cells[column].innerText.trim();
        const rightValue = right.row.cells[column].dataset.sortValue ?? right.row.cells[column].innerText.trim();
        if (!leftValue || !rightValue) {
          if (!leftValue && !rightValue) return left.index - right.index;
          return !leftValue ? 1 : -1;
        }
        const leftNumber = Number(leftValue);
        const rightNumber = Number(rightValue);
        const comparison = Number.isFinite(leftNumber) && Number.isFinite(rightNumber)
          ? leftNumber - rightNumber
          : leftValue.localeCompare(rightValue, undefined, { numeric: true, sensitivity: "base" });
        return (ascending ? comparison : -comparison) || left.index - right.index;
      });
      rows.forEach(item => table.tBodies[0].appendChild(item.row));
    };
    header.addEventListener("click", sort);
    header.addEventListener("keydown", event => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        sort();
      }
    });
  });
}
for (const button of document.querySelectorAll(".copy-button")) {
  button.addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText(button.dataset.copy);
    } catch {
      const textarea = document.createElement("textarea");
      textarea.value = button.dataset.copy;
      document.body.appendChild(textarea);
      textarea.select();
      document.execCommand("copy");
      textarea.remove();
    }
    button.classList.add("copied");
    setTimeout(() => button.classList.remove("copied"), 1200);
  });
}
const logDrawer = document.querySelector(".log-drawer");
const logOutput = document.querySelector(".log-output");
const logTitle = document.querySelector(".log-title");
let logEvents = null;
for (const button of document.querySelectorAll(".log-button")) {
  button.addEventListener("click", () => {
    logEvents?.close();
    logOutput.textContent = "Connecting to CloudWatch…\n";
    logTitle.textContent = `Live logs · ${button.dataset.jobName} · job ${button.dataset.jobId}`;
    logDrawer.classList.add("open");
    logDrawer.setAttribute("aria-hidden", "false");
    logEvents = new EventSource(`/api/logs/${button.dataset.jobId}`);
    logEvents.onmessage = event => {
      const item = JSON.parse(event.data);
      const timestamp = new Date(item.timestamp).toLocaleTimeString();
      if (logOutput.textContent.startsWith("Connecting")) logOutput.textContent = "";
      logOutput.insertAdjacentHTML("beforeend", `[${timestamp}] ${item.html}\n`);
      logOutput.scrollTop = logOutput.scrollHeight;
    };
    logEvents.onerror = () => {
      if (!logOutput.textContent.endsWith("Reconnecting…\n")) logOutput.textContent += "\nReconnecting…\n";
    };
  });
}
function closeLiveLogDrawer() {
  logEvents?.close();
  logEvents = null;
  logDrawer.classList.remove("open");
  logDrawer.setAttribute("aria-hidden", "true");
}
document.querySelector(".log-close").addEventListener("click", closeLiveLogDrawer);
document.addEventListener("keydown", event => {
  if (event.key === "Escape" && logDrawer.classList.contains("open")) closeLiveLogDrawer();
});
let serviceState = null;
setInterval(async () => {
  try {
    const nextState = await (await fetch("/api/health", { cache: "no-store" })).json();
    if (serviceState && (nextState.startup_id !== serviceState.startup_id || nextState.report_version !== serviceState.report_version) && !logDrawer.classList.contains("open")) location.reload();
    serviceState = nextState;
  } catch { /* The debug reloader briefly takes the local service offline. */ }
}, 2000);
