
class BaseModel():
    def __init__(self, attributes):
        for attribute in attributes:
            setattr(self, attribute, None)

class ModelSSQ(BaseModel):
    def __init__(self):
        super(ModelSSQ).__init__(['sequence', 'date', 'r1', 'r2', 'r3', 'r4', 'r5', 'r6', 'b1', 'input', 'pool', 'amount_1st', 'value_1st', 'amount_2nd', 'value_2nd', 'amount_3rd', 'value_3rd', 'amount_4th', 'value_4th', 'amount_5th', 'value_5th', 'amount_6th', 'value_6th'])

class ModelDLT(BaseModel):
    def __init__(self):
        super(ModelSSQ).__init__(['sequence', 'date', 'r1', 'r2', 'r3', 'r4', 'r5', 'r6', 'b1', 'input', 'pool', 'amount_1st', 'value_1st', 'amount_2nd', 'value_2nd', 'amount_3rd', 'value_3rd', 'amount_4th', 'value_4th', 'amount_5th', 'value_5th', 'amount_6th', 'value_6th'])
