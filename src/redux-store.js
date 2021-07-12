import { createStore, applyMiddleware } from 'redux'
import thunk from 'redux-thunk'

import { canSelectEvents } from './selectors'

//--> Actions
const actions = {
  FETCH_USERS_SUCCESS: 'fetch_users_success', // really doesn't matter what the values are
  FETCH_USERS_ERROR: 'fetch_users_error',
  CHANGE_SELECTED_USER_ID: 'change_selected_user_id',
  FETCH_ADDRESS_SUCCESS: 'fetch_address_success',
  FETCH_ADDRESS_ERROR: 'fetch_address_error',
  REQUEST_ADDRESS_DETAILS: 'request_address_details',
  FETCH_EVENTS_SUCCESS: 'fetch_events_success',
  FETCH_EVENTS_ERROR: 'fetch_events_error',
  TOGGLE_EVENT_SELECTION: 'toggle_event_selection',
  EVENT_DETAILS_SUCCESS: 'event_details_success',
  EVENT_DETAILS_ERROR: 'event_details_error'
}


//--> Reducer
const reducer = (state, action) => {
  switch (action.type) {
    case actions.FETCH_USERS_SUCCESS:
      return {
        ...state,
        userIds : action.payload
      }
    case actions.FETCH_USERS_ERROR:
      return {
        ...state,
        error : "Something went wrong while fetching users."
      }
    case actions.CHANGE_SELECTED_USER_ID:
      return {
        ...state,
        addressesLoading : true,
        addresses : [],
        events : [],
        selectedUserId : action.payload,
        selectedAddressId : null,
        selectedEvents : {}
      }
    case actions.FETCH_ADDRESS_SUCCESS:
      return {
        ...state,
        addressesLoading : false,
        addresses : action.payload,
        error: null
      }
    case actions.FETCH_ADDRESS_ERROR:
      return {
        ...state,
        addressesLoading : false,
        addresses: [],
        error : "Something went wrong while fetching addresses."
      }
    case actions.REQUEST_ADDRESS_DETAILS:
      return {
        ...state,
        eventsLoading : true,
        events : [],
        selectedAddressId: action.payload
      }
    case actions.FETCH_EVENTS_SUCCESS:
      return {
        ...state,
        addressesLoading : false,
        events : action.payload
      }
    case actions.FETCH_EVENTS_ERROR:
      return {
        ...state,
        addressesLoading : false,
        error : "Something went wrong while fetching events for this address."
      }
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

      return {
        ...state,
        selectedEvents : events
      }
    case actions.COMPARE_SELECTED_EVENTS:
      return {
        ...state,
        comparingEvents : true
      }
    case actions.EVENT_DETAILS_SUCCESS:
      return {
        ...state,
        comparisonJson : action.payload
      }
    case actions.EVENT_DETAILS_ERROR:
      return {
        ...state,
        error : action.payload
      }
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
  selectedEvents: {},
  error: null
}

const store = createStore(reducer, initialState, applyMiddleware(thunk))

export { store, actions }
