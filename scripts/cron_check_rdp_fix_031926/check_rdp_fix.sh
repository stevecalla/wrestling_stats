#!/bin/bash

BUG_URL="https://bugs.launchpad.net/ubuntu/+source/gnome-remote-desktop/+bug/2144967"
CHAT_GPT_URL="https://chatgpt.com/c/69bc7e6f-743c-83e8-ab23-8cfc16d1d366"

KNOWN_GOOD="3.5.0+dfsg1-0ubuntu1"
KNOWN_BAD="3.5.1+dfsg1-0ubuntu1.4"

# LOG FILE
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="${SCRIPT_DIR}/check_rdp_fix.log"

# Clear prior log and send all output to new log + console
: > "$LOG_FILE"
exec > >(tee -a "$LOG_FILE") 2>&1

# TEST MODE - STEP 1
# Set to 1 to simulate an available candidate version for testing script output
TEST_MODE=0

# TEST MODE - STEP 2
# If TEST_MODE=0 this is ignored
# If TEST_MODE=1, set the simulated candidate version here
TEST_VERSION="3.5.1+dfsg1-0ubuntu1.5"

PACKAGES="libfreerdp3-3 libfreerdp-server3-3 libfreerdp-client3-3 libwinpr3-3"

echo "===== $(date) ====="
echo "Checking FreeRDP / WinPR status..."
echo "Log file: ${LOG_FILE}"
echo "Context: GNOME RDP regression workaround is currently in place"
echo "Reference bug: ${BUG_URL}"
echo "Reference chatgpt: ${CHAT_GPT_URL}"
echo ""

echo "📦 Installed versions (REGRESSION FIX - HELD):"
dpkg-query -W -f=' - ${Package}: ${Version}\n' $PACKAGES 2>/dev/null

echo ""
echo "🔒 Hold status:"
HOLD_OUTPUT=$(apt-mark showhold | grep -E 'freerdp|winpr')
if [ -n "$HOLD_OUTPUT" ]; then
  echo "$HOLD_OUTPUT" | awk '{print " - " $0}'
else
  echo " - No holds found (⚠️ unexpected)"
fi

echo ""
echo "🧭 Version reference:"
echo " - Known good rollback version: ${KNOWN_GOOD}"
echo " - Known broken regression version: ${KNOWN_BAD}"
echo " - TEST_MODE: ${TEST_MODE}"
echo " - TEST_VERSION: ${TEST_VERSION}"
if [ "$TEST_MODE" -eq 1 ]; then
  echo " - Test mode enabled with simulated candidate version: ${TEST_VERSION}"
fi

echo ""

if [ "$TEST_MODE" -eq 1 ]; then
  CANDIDATE_VERSION="${TEST_VERSION}"
else
  CANDIDATE_VERSION=$(apt-cache policy libfreerdp3-3 | awk '/Candidate:/ {print $2}')
fi

INSTALLED_VERSION=$(dpkg-query -W -f='${Version}\n' libfreerdp3-3 2>/dev/null)

echo "🔍 Version check:"
echo " - Installed version: ${INSTALLED_VERSION}"
echo " - Candidate version: ${CANDIDATE_VERSION}"
echo ""

if [ -z "$CANDIDATE_VERSION" ] || [ "$CANDIDATE_VERSION" = "(none)" ]; then
  echo "⚠️ Could not determine candidate version"
  echo "👉 Keep packages on hold"
  exit 0
fi

if [ "$CANDIDATE_VERSION" = "$INSTALLED_VERSION" ]; then
  echo "✅ No newer candidate available — remaining on regression-safe version"
  echo "👉 Keep packages on hold"
  exit 0
fi

if [ "$CANDIDATE_VERSION" = "${KNOWN_BAD}" ]; then
  echo "⚠️ Candidate version is still the KNOWN BROKEN version (${KNOWN_BAD})"
  echo "👉 DO NOT upgrade"
  echo "👉 Keep packages on hold"
  echo "👉 Continue monitoring bug: ${BUG_URL}"
  exit 0
fi

echo "🚨 Candidate new version detected:"
echo " - ${CANDIDATE_VERSION}"
echo ""

echo "🧪 How to determine whether this might fix the issue:"
echo " - Check the Launchpad bug above for status changes or maintainer comments"
echo " - Look for references to freerdp, winpr, rdp negotiation, segfault, or regression fix"
echo " - Treat candidate version ${CANDIDATE_VERSION} as a candidate fix only until tested"
echo " - Do not remove the hold across all machines at once"

echo ""
echo "👉 Recommended next steps:"
echo "  1. Review the bug/changelog for the candidate version"
echo "  2. Remove hold on ONE machine only"
echo "  3. Upgrade only the FreeRDP / WinPR packages"
echo "  4. Restart gnome-remote-desktop"
echo "  5. Test RDP from the Mac"
echo "  6. Watch logs with:"
echo "     journalctl --user -u gnome-remote-desktop -f"
echo "  7. Confirm there are NO:"
echo "     - corrupted size vs. prev_size"
echo "     - SEGV"
echo "     - ABRT"
echo "     - core-dump restart loop"

echo ""
echo "✅ Success criteria:"
echo " - RDP connects successfully"
echo " - gnome-remote-desktop stays running"
echo " - no crash signatures appear in the log"

echo ""
echo "↩️ Rollback plan if still broken:"
echo "  1. Downgrade back to known good version:"
echo "     sudo apt install --allow-downgrades \\"
echo "       libfreerdp3-3=${KNOWN_GOOD} \\"
echo "       libfreerdp-server3-3=${KNOWN_GOOD} \\"
echo "       libfreerdp-client3-3=${KNOWN_GOOD} \\"
echo "       libwinpr3-3=${KNOWN_GOOD}"
echo ""
echo "  2. Re-apply package holds immediately after rollback:"
echo "     sudo apt-mark hold \\"
echo "       libfreerdp3-3 \\"
echo "       libfreerdp-server3-3 \\"
echo "       libfreerdp-client3-3 \\"
echo "       libwinpr3-3"
echo ""
echo "  3. Restart GNOME Remote Desktop:"
echo "     systemctl --user restart gnome-remote-desktop"e