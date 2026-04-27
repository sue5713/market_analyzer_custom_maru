/**
 * Google Apps Script for Triggering GitHub Action from Google Form
 *
 * SECURITY: The GitHub token is read from Apps Script's Script Properties
 *           at runtime. Never paste a token in this file.
 *
 * Setup (one-time):
 * 1. Apps Script editor → Project Settings → Script properties → Add property
 *      Name:  GITHUB_TOKEN
 *      Value: <fine-grained PAT with Actions: read/write on this repo>
 * 2. Triggers → Add Trigger → onFormSubmit / From form / On form submit.
 */

const CONFIG = {
  GITHUB_OWNER: 'sue5713',
  GITHUB_REPO: 'market_analyzer_custom_maru',
  WORKFLOW_FILE: 'market_analysis.yml',
  BRANCH: 'main'
};

function onFormSubmit(e) {
  const itemResponses = e.response.getItemResponses();
  let startDate = '';
  let endDate = '';

  for (var i = 0; i < itemResponses.length; i++) {
    var itemResponse = itemResponses[i];
    var title = itemResponse.getItem().getTitle();
    var response = itemResponse.getResponse();

    if (title.includes('Start')) {
      startDate = response;
    } else if (title.includes('End')) {
      endDate = response;
    }
  }

  Logger.log('Start: ' + startDate);
  Logger.log('End: ' + endDate);

  if (startDate && endDate) {
    triggerGitHubAction(startDate, endDate);
  } else {
    Logger.log('Dates not found in form response.');
  }
}

function triggerGitHubAction(start, end) {
  const githubToken = PropertiesService.getScriptProperties().getProperty('GITHUB_TOKEN');
  if (!githubToken) {
    throw new Error('GITHUB_TOKEN script property is not set. Configure it in Project Settings → Script properties.');
  }

  const url = `https://api.github.com/repos/${CONFIG.GITHUB_OWNER}/${CONFIG.GITHUB_REPO}/actions/workflows/${CONFIG.WORKFLOW_FILE}/dispatches`;

  const payload = {
    ref: CONFIG.BRANCH,
    inputs: {
      start_date: start,
      end_date: end
    }
  };

  const options = {
    method: 'post',
    headers: {
      'Authorization': `Bearer ${githubToken}`,
      'Accept': 'application/vnd.github+json',
      'Content-Type': 'application/json'
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };

  try {
    const response = UrlFetchApp.fetch(url, options);
    const code = response.getResponseCode();
    Logger.log('GitHub Action Triggered: ' + code);
    if (code !== 204) {
      Logger.log('Body: ' + response.getContentText());
    }
  } catch (error) {
    Logger.log('Error triggering GitHub Action: ' + error.toString());
  }
}
