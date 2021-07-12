import { actions } from './redux-store';

const API_BASE = 'http://localhost:5000'

const fetchUserIds = () => (dispatch) => {
  return fetch(`${API_BASE}/user_ids`).then((response) => {
    if (!response.ok) {
      return dispatch({
        type: actions.FETCH_USERS_ERROR,
      })
    }

    return response.json
  }, err => {
    throw err
  }).then(data => {
    return dispatch({
      type: actions.FETCH_USERS_SUCCESS,
      payload: data
    })
  }, err => {
    return dispatch({
      type: actions.FETCH_USERS_ERROR
    })
  })
}

const fetchAddresses = (userId) => (dispatch) => {
  return fetch(`${API_BASE}/users/${userId}/addresses`).then((response) => {
    if (!response.ok) {
      return dispatch({
        type: actions.FETCH_ADDRESS_ERROR,
      })
    }

    return response.json()
  }, err => {
    throw err
  }).then(data => {
    return dispatch({
      type: actions.FETCH_ADDRESS_SUCCESS,
      payload: data
    })
  }, err => {
    return dispatch({
      type: actions.FETCH_ADDRESS_ERROR
    })
  })
}

const fetchEvents = (addressId) => (dispatch) => {
  return fetch(`${API_BASE}/addresses/${addressId}/events`).then((response) => {
    if (!response.ok) {
      return dispatch({
        type: actions.FETCH_EVENTS_ERROR,
      })
    }

    return response.json()
  }, err => {
    throw err
  }).then(data => {
    return dispatch({
      type: actions.FETCH_EVENTS_SUCCESS,
      payload: data
    })
  }, err => {
    return dispatch({
      type: actions.FETCH_EVENTS_ERROR
    })
  })
}

const fetchSelectedEventDetails = () => (dispatch, getState) => {
  const { selectedEvents, events } = getState()
  return Promise.all(
    events.filter(event => {
      return !!selectedEvents[event.created_at + '-' + event.id]
    }).map(event => {
      return fetch(API_BASE + event.url).then((response) => {
        if (!response.ok) {
          throw new Error('Failed request');
        }
        return response.json()
      }, err => {
        throw err
      })
    })
  ).then(values => {
    return dispatch({
      type: actions.EVENT_DETAILS_SUCCESS,
      payload: values
    })
  }).catch(err => {
    return dispatch({
      type: actions.EVENT_DETAILS_ERROR,
      payload: err
    })
  })
}

export { fetchUserIds, fetchAddresses, fetchEvents, fetchSelectedEventDetails }
