import logging

from datetime import datetime, timedelta

from flask import abort, jsonify
from sqlalchemy.sql.sqltypes import NULLTYPE
from webargs.flaskparser import use_args

from marshmallow import Schema, fields

from service.server import app, db
from service.models import AddressSegment
from service.models import Person


class GetAddressQueryArgsSchema(Schema):
    date = fields.Date(required=False, missing=datetime.utcnow().date())


class AddressSchema(Schema):
    class Meta:
        ordered = True

    street_one = fields.Str(required=True, max=128)
    street_two = fields.Str(max=128)
    city = fields.Str(required=True, max=128)
    state = fields.Str(required=True, max=2)
    zip_code = fields.Str(required=True, max=10)

    start_date = fields.Date(required=True)
    end_date = fields.Date(required=False)


@app.route("/api/persons/<uuid:person_id>/address", methods=["GET"])
@use_args(GetAddressQueryArgsSchema(), location="querystring")
def get_address(args, person_id):
    person = Person.query.get(person_id)
    if person is None:
        abort(404, description="person does not exist")
    elif len(person.address_segments) == 0:
        abort(404, description="person does not have an address, please create one")

    address_segment = person.address_segments[-1]
    return jsonify(AddressSchema().dump(address_segment))


@app.route("/api/persons/<uuid:person_id>/address", methods=["PUT"])
@use_args(AddressSchema())
def create_address(payload, person_id):
    person = Person.query.get(person_id)
    if person is None:
        abort(404, description="person does not exist")
    # If there are no AddressSegment records present for the person, we can go
    # ahead and create with no additional logic.
    elif len(person.address_segments) == 0:
        address_segment = AddressSegment(
            street_one=payload.get("street_one"),
            street_two=payload.get("street_two"),
            city=payload.get("city"),
            state=payload.get("state"),
            zip_code=payload.get("zip_code"),
            start_date=payload.get("start_date"),
            person_id=person_id,
        )

        db.session.add(address_segment)
        db.session.commit()
        db.session.refresh(address_segment)
    else:
        # TODO: Implementation
        # If there are one or more existing AddressSegments, create a new AddressSegment
        # that begins on the start_date provided in the API request and continues
        # into the future. If the start_date provided is not greater than most recent
        # address segment start_date, raise an Exception.


        #loop through all the previous addresses to find the most recent one
        for address in person.address_segments:

            if address.start_date > payload.get("start_date"):
                raise ValueError('start_date must be greater than previous address start_date')
            #if the start date is null, then you have the latest record
            if address.end_date == payload.get("end_date"):

                #update end date to date of new start date
                address.end_date = payload.get("start_date")

                #push changes to old address
                db.session.add(address)
                db.session.commit()
                db.session.refresh(address)
                break

        #create new address with null end date, meaning its the most active address
        address_segment = AddressSegment(
            street_one=payload.get("street_one"),
            street_two=payload.get("street_two"),
            city=payload.get("city"),
            state=payload.get("state"),
            zip_code=payload.get("zip_code"),
            start_date=payload.get("start_date"),
            person_id=person_id,
        )
        #push new address to DB
        db.session.add(address_segment)
        db.session.commit()
        db.session.refresh(address_segment)

        '''I have reached my stopping point, I have the right idea but my syntax is iffy. The old address gets an end date but
        when I call this method @app.route("/api/persons/<uuid:person_id>/address", methods=["GET"]) it is returning the old address not the new one. Which means the new address isnt getting saved'''

    return jsonify(AddressSchema().dump(address_segment))
