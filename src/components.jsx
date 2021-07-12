import React from 'react'
import { connect } from 'react-redux'

import { fetchAddresses, fetchEvents, fetchUserIds /* , fetchSelectedEventDetails */} from './thunks'
import { eventGuid, canSelectEvents, undeletedAddresses } from './selectors'
import { actions } from './redux-store'

//const API_BASE = "http://localhost:5000"

//--> User select form
const submitHandler = (dispatch, userId) => (e) => {
  e.preventDefault()

  dispatch({
    type: actions.CHANGE_SELECTED_USER_ID,
    payload: userId
  })
}

const changeHandler = (dispatch) => (e) => {
  const val = e.target.value

  dispatch({
    type: actions.CHANGE_SELECTED_USER_ID,
    payload: val
  })
  dispatch(fetchAddresses(val))
}

let UserSelectForm = ({ dispatch, userIds, selectedUserId }) => {
  //userIds = ["a0904e77-a91d-4562-807b-9e223ab00625","ea66ed01-7cbd-4f13-9a03-796a26e55e95"]
    return <form  onSubmit={submitHandler(dispatch, userIds)}>
    <select onChange={changeHandler(dispatch)} value={selectedUserId || ''}>
      <option>Select User ID</option>
        {userIds.map(id => {
        return <option key={id} value={id}>{id}</option>
      })}
    </select>
  </form> 
}
UserSelectForm = connect(state => state)(UserSelectForm)




//--> Events list
const handleEventToggle = (dispatch, guid) => (e) => {
  dispatch({
    type: actions.TOGGLE_EVENT_SELECTION,
    payload: guid
  })
}
let Event = ({ dispatch, event, guid, isSelected, isEnabled }) => {
  return <li>
    <input id={guid} type="checkbox" checked={isSelected} disabled={!isEnabled} onChange={handleEventToggle(dispatch, guid)} />
    <label htmlFor={guid}>
      {event.type} | {event.created_at}
    </label>
  </li>
}
Event = connect((state, ownProps) => {
  const isSelected = !!state.selectedEvents[ownProps.guid]
  return {
    isSelected : isSelected,
    isEnabled : isSelected || canSelectEvents(state.selectedEvents)
  }
})(Event)


const handleCompareClick = (dispatch) => (e) => {
  /*
   * Your code here (and probably elsewhere)
   *
   *
   * We've provided a thunk function to fetch
   * event data.
   * Find it in thunks.js, lines 81-107,
   * and referenced in the comment below on line 78.
   */

  // dispatch(fetchSelectedEventDetails())
}

let EventList = ({dispatch, canCompare, events}) => {
  return <>
    <button onClick={handleCompareClick(dispatch)} disabled={!canCompare}>Compare</button>
    <ul>
      {events.map((event) => {
        return <Event event={event} key={eventGuid(event)} guid={eventGuid(event)} />
      })}
    </ul>
  </>
}
EventList = connect(state => {
  return { canCompare : Object.keys(state.selectedEvents).length > 1 }
})(EventList)



//--> Addresses list
const handleAddressClick = (dispatch, id) => (e) => {
  e.preventDefault()

  dispatch({
    type: actions.REQUEST_ADDRESS_DETAILS,
    payload: id
  })
  dispatch(fetchEvents(id))
}


let Address = ({ dispatch, addressJson, isSelected }) => {
  return <li onClick={handleAddressClick(dispatch, addressJson.id)} className={isSelected ? 'selected' : ''}>
    <pre>{JSON.stringify(addressJson, undefined, 2)}</pre>
  </li>
}
Address = connect((state, ownProps) => {
  return { isSelected : state.selectedAddressId === ownProps.addressJson.id }
})(Address)



//--> App wrapper
let App = ({ addresses, events, userIds, selectedUserId, selectedAddressId, comparingEvents, error} ) => {
  //userIds = ["a0904e77-a91d-4562-807b-9e223ab00625","ea66ed01-7cbd-4f13-9a03-796a26e55e95"]
  //console.log(userIds)
  return <>
    {error ? <p className="error">{error}</p> : ''}
    {userIds && userIds.length ?
      <UserSelectForm userIds={userIds} selectedUserId={selectedUserId} />
    : 'no users found'}
    <div className="addresses">
      <h2>Address Information</h2>
      {addresses && addresses.length
        ? <ul>
            {addresses.map((address) => {
              return <Address key={address.id} addressJson={address} />
            })}
          </ul>
        : <p>{selectedUserId ? 'No addresses found.' : 'Choose a user ID from the dropdown above.'}</p>
      }
    </div>
    <div className="events">
      <h2>Events</h2>
      { events && events.length
        ? <EventList events={events} />
        : <p>{selectedAddressId ? 'No events found.' : 'Select an address to see events'}</p>
      }
    </div>
  </>
}
App = connect(state => {
  console.log(state)
  return {
    addresses : undeletedAddresses(state.addresses)
    //userIds : state.userIds
  }
})(App)


export { App }
