const MAX_SELECTED_EVENTS = 2

const eventGuid = (evt) => {
  return evt.created_at + "-" + evt.id
}

const canSelectEvents = (selectedEvents) => {
  return Object.keys(selectedEvents).length < MAX_SELECTED_EVENTS
}


const undeletedAddresses = (addresses) => {
  return addresses.filter(address => {
    return address.deleted_at === null
  })
}

const selectedEventData = (selectedEvents, events) => {
  return events.filter((evt) => {
    return !!selectedEvents[eventGuid(evt)]
  })
}

export { eventGuid, canSelectEvents, undeletedAddresses, selectedEventData }
