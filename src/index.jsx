import React from 'react'
import ReactDOM from 'react-dom'
import { createStore, applyMiddleware } from 'redux'
import thunk from 'redux-thunk'
import { Provider } from 'react-redux'

import { canSelectEvents } from './selectors'
import actions from './actions'
import { fetchUserIds } from './thunks'
import { App } from './components.jsx'

/**
 *
 * REDUX
 *
 * */


//--> Reducer
const reducer = (state, action) => {
  switch (action.type) {
    case actions.FETCH_USERS_SUCCESS:
      return Object.assign({}, state, {
        userIds : action.payload
      })
    case actions.FETCH_USERS_ERROR:
      return Object.assign({}, state, {
        error : "Something went wrong while fetching users."
      })
    case actions.CHANGE_SELECTED_USER_ID:
      return Object.assign({}, state, {
        addressesLoading : true,
        addresses : [],
        events : [],
        selectedUserId : action.payload,
        selectedAddressId : null,
        selectedEvents : {}
      })
    case actions.FETCH_ADDRESS_SUCCESS:
      return Object.assign({}, state, {
        addressesLoading : false,
        addresses : action.payload
      })
    case actions.FETCH_ADDRESS_ERROR:
      return Object.assign({}, state, {
        addressesLoading : false,
        error : "Something went wrong while fetching addresses."
      })
    case actions.REQUEST_ADDRESS_DETAILS:
      return Object.assign({}, state, {
        eventsLoading : true,
        events : [],
        selectedAddressId: action.payload
      })
    case actions.FETCH_EVENTS_SUCCESS:
      return Object.assign({}, state, {
        addressesLoading : false,
        events : action.payload
      })
    case actions.FETCH_EVENTS_ERROR:
      return Object.assign({}, state, {
        addressesLoading : false,
        error : "Something went wrong while fetching events for this address."
      })
    case actions.TOGGLE_EVENT_SELECTION:
      const eventId = action.payload
      const events = Object.assign({}, state.selectedEvents)
      if (state.selectedEvents[eventId]) {
        delete events[eventId]
      } else 
      // only allowed to compare 2 events
      if (canSelectEvents(state.selectedEvents)) {
        events[eventId] = true
      }

      return Object.assign({}, state, {
        selectedEvents : events
      })
    case actions.COMPARE_SELECTED_EVENTS:
      return Object.assign({}, state, {
        comparingEvents : true
      })
    case actions.EXIT_COMPARE_MODE:
      return Object.assign({}, state, {
        comparingEvents : false,
        comparisonJson : null // TODO: I could cache here
      })
    case actions.EVENT_DETAILS_SUCCESS:
      return Object.assign({}, state, {
        comparisonJson : action.payload
      })
    case actions.EVENT_DETAILS_ERROR:
      return Object.assign({}, state, {
        error : action.payload
      })
    default:
      return state
  }
}


//--> Setup
const initialState = {
  addresses: [],
  events: [],
  userIds: [],
  selectedUserId: null,
  selectedAddressId: null,
  selectedEvents: {}
}

const store = createStore(reducer, initialState, applyMiddleware(thunk))






/**
 *
 * App initialization
 *
 */
const rootElement = document.getElementById('root')

store.dispatch(fetchUserIds())
ReactDOM.render(
  <Provider store={store}>
    <App />
  </Provider>,
  rootElement
)
