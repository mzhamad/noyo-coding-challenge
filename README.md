# Noyo UI coding challenge

## Background
One of our biggest challenges at Noyo is understanding how our data changes over time and why. This allows us to understand the state of our system when a given transaction was executed, and to inspect how and why the data changed since the request was made. It allows us to answer important questions such as:

* What was the exact address on file for an employee when a transaction was performed X days ago?
* What exactly was changed about the mailing address of this employee when the last update occurred?

We've included here a small React application which is meant to display this sort of information, but it's not quite complete. We're asking you to complete it for us.

## What we're evaluating
* Whenever you start a new job, you encounter code other people wrote, and you'll have to understand the architecture and write code to match it.
* How do you approach writing a new feature? Do you write your own code or use a library? How do you decide on a good libary if you choose to use one? There are no right or wrong answers here, but we're interested in your thinking (and the results).
* Does your code do what it's supposed to do without obvious errors or bugs?
* Can you debug a problem in some unfamiliar code? How did you go about debugging?
* Do you have a basic command of CSS layout?

## What we're not evaluating
* *Your design skills.* Our designers welcome your feedback, but we don't expect our devs to also be designers.
* *Your ability to bootstrap a new application.* Hopefully you don't have to do this many times at Noyo, which is why we've provided a running application.
* *Building things from scratch* with no code to work from.
* *Automated testing*: We believe this is an important part of development work, but are not expecting it for this assignment, unless you find yourself with extra time. We do expect fairly thorough manual testing.
* *Your commit messages or git hygiene*. In this case, you're not really collaborating with anyone, so we don't expect you to pretend you are. However you work when you're working by yourself is fine.



## Getting Started

### Basic setup
We recommend you use Node v14.15.1 for this assignment, but we expect that any version beginning with v12.0.0 will work.

 After you clone this repo, you'll need to run `npm install` (once), and then run
`npm start`. 

This repo was generated with [create-react-app](https://github.com/facebook/create-react-app) and thus uses [webpack-dev-server](https://github.com/webpack/webpack-dev-server).
When you run `npm start`, webpack-dev-server will open a new browser tab or window with the running app.
Changes you make will be compiled automatically, and you should see the page refresh.

*Estimated setup time: 15 minutes.*

### Key tools and concepts

Our app uses [Redux](https://redux.js.org/) and [Redux Thunk](https://github.com/reduxjs/redux-thunk). Here's a [good explanation of thunks](https://daveceddia.com/what-is-a-thunk/).

*We recognize that it may take some time to familiarize yourself with these tools*

### Interfacing with Noyo's APIs

The application uses [this address API](https://github.com/noyo-technologies/address-history-challenge). You'll have to follow the instructions here to get it running on your own computer and seed the data, but you'll find that all the API queries have already been supplied for you in the JavaScript code. 

## Your tasks
### 1. Find and fix a bug in our application
When you start the application, you should see text that says **"Choose a user ID from the dropdown above."**

But you won't see anything in the UI that allows you to select a user ID. What gives?

If you dig into the code, you'll see that we didn't forget to implement this; there's a component called `UserSelectForm` in `src/components.jsx` (on line 29).

So why isn't this displaying when you load the app? Your first task is to diagnose and fix the issue displaying our user select component. Please be prepared to discuss your debugging process during the interview process.

*Estimated time to complete: 15 minutes to 1 hour*



### 2. Add a feature that makes it possible to compare two events associated with a given address.
Once you've fixed the bug above, you should be able to select a user ID from the dropdown.

For each user, you'll see one or more addresses listed on the left. If you select one of these, you should see a list of events on the right, along with a button marked "compare".

If you select two events, and click "compare", you'll see that nothing happens.

We'd like you to implement a small feature that lets you compare the JSON contents of any two events in the list. 

- The event comparison should appear in an overlay or a modal dialog box. 
- You should be able to exit event comparison mode using a button or link.
- The events should be displayed side-by-side, with properties or values that differ clearly highlighted (imagine a side-by-side git diff).
- You may use third-party libraries for displaying the event JSON and for highlighting differences, or you may write your own solution. Either way, be prepared to discuss how you made your decisions.
- You're free to style it however you feel is best, and we expect competent CSS, but you *will not be evaluated on your design skills*.

To get started, take a look at lines 67-79 of `src/components.jsx`.

*Estimated time to complete: 1-3 hours*


### 3. Add retry logic for cases when the API connection is lost.

Try the following scenario:

1. With the app running, shut down the API server.
2. Try to select a different user ID in the users dropdown.
3. You should see an error message reading "Something went wrong while fetching addresses."
4. Restart the server.
5. Once it's running, try to select a user ID again.
6. You should see the error message go away and the app return to normal function.

So far, so good. Now try this scenario:

1. Shut down the API server.
2. Refresh the app in your browser.
3. You should see an error message reading "Something went wrong while fetching users."
4. Restart the server.
5. The only way you can get the app back to working order is to refresh the page. We want to fix this.

We work really hard to make sure our apps and our APIs have excellent uptime, but cloud platforms like GCS and AWS have occasional outages. Usually these are brief.

As such, we want this app to try to recover if it starts up and fails to connect to the API server. What we'd like you to do is implement some retry logic with the following requirements:

* If the API request to fetch user IDs (`index.js`, line 19) fails with an error in the 500 range or the fetch request fails altogether, our app should retry this request.
* It should retry up to 4 times (5 requests in total).
* There should be 10 seconds between each retry.
* If any of the retry attempts succeeds, we should not retry again. The app should function normally at this point.
* If the status code of any request is in the 400 range, we should treat this as success for the purposes of our retry logic, but we should still show an error message.
* The retry logic should only apply to fetching user IDs, not addreses or events.

*Estimated time to complete: 30 minutes to 1 hour*


## Time management:
We've provided time estimates for each of the tasks above, and we've attempted to test our assumptions. However, we recognize that everyone works differently. Coding may involve race conditions, but it's not a race. **We do NOT believe speed is the best measure of proficiency**.

As such, we ask that you please limit your time spent on these tasks to 5 hours. To help with this, we tried to clearly state above what we are and aren't evaluating in your submission. **If you are unable to complete all the tasks within 5 hours, that's perfectly fine**. We prefer that you tackle tasks in the order in which they're listed in this document.

If you identify additions or changes you'd like to make to your code that are "nice-to-haves" or otherwise feel unnecessary to a working solution, we suggest you leave these until the end. You may want to keep a small list of items you'd like to address if you had more time; we can talk about them in the interview.

## Thanks!!
We make every effort to demonstrate that we see your time as valuable, and we will make every attempt throughout the interview process not to waste it. Nonetheless, we know you're making a big time commitment and we're sincerely appreciative of both your time and your interest in working at Noyo.