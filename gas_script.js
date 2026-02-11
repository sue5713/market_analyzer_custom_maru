/**
 * Google Apps Script for Triggering GitHub Action from Google Form
 * 
 * Instructions:
 * 1. Open your Google Form.
 * 2. Click the 3 dots menu -> Script editor.
 * 3. Paste this code into the editor (replace existing code).
 * 4. Set the CONFIG variables below.
 * 5. Save the script.
 * 6. Set up a Trigger:
 *    - Click 'Triggers' (alarm clock icon) on the left.
 *    - Click '+ Add Trigger'.
 *    - Choose function to run: 'onFormSubmit'.
 *    - Select event source: 'From form'.
 *    - Select event type: 'On form submit'.
 *    - Save.
 */

const CONFIG = {
  GITHUB_OWNER: 'sue5713',       // Your GitHub Username
  GITHUB_REPO: 'market_analyzer_custom', // Your Repository Name
  WORKFLOW_FILE: 'market_analysis.yml',  // The workflow filename
  GITHUB_TOKEN: 'YOUR_GITHUB_PAT_HERE'   // PASTE YOUR GITHUB PAT HERE (Keep the quotes)
};

function onFormSubmit(e) {
  // Get form responses
  const itemResponses = e.response.getItemResponses();
  let startDate = "";
  let endDate = "";

  // Assumes the form has questions titled "Start Date" and "End Date"
  // Format expected by user input: "YYYY-MM-DD HH:MM" 
  // (Or you can use Date/Time pickers and format them here)
  
  for (var i = 0; i < itemResponses.length; i++) {
    var itemResponse = itemResponses[i];
    var title = itemResponse.getItem().getTitle();
    var response = itemResponse.getResponse();

    if (title.includes("Start")) {
      startDate = response; 
    } else if (title.includes("End")) {
      endDate = response;
    }
  }

  Logger.log("Start: " + startDate);
  Logger.log("End: " + endDate);

  if (startDate && endDate) {
    triggerGitHubAction(startDate, endDate);
  } else {
    Logger.log("Dates not found in form response.");
  }
}

function triggerGitHubAction(start, end) {
  const url = `https://api.github.com/repos/${CONFIG.GITHUB_OWNER}/${CONFIG.GITHUB_REPO}/actions/workflows/${CONFIG.WORKFLOW_FILE}/dispatches`;
  
  const payload = {
    ref: 'main', // Branch to use
    inputs: {
      start_date: start,
      end_date: end
    }
  };

  const options = {
    method: 'post',
    headers: {
      'Authorization': `token ${CONFIG.GITHUB_TOKEN}`,
      'Accept': 'application/vnd.github.v3+json',
      'Content-Type': 'application/json'
    },
    payload: JSON.stringify(payload)
  };

  try {
    const response = UrlFetchApp.fetch(url, options);
    Logger.log("GitHub Action Triggered: " + response.getResponseCode());
  } catch (error) {
    Logger.log("Error triggering GitHub Action: " + error.toString());
  }
}
