import MNN


def load_mnn_model(model_file: str):
    """Load MNN model from file and use for Training
    params: model_file: FILEPATH of MNN model
    """
    var_map = MNN.expr.load_as_dict(model_file)

    # for k, var in var_map.items():
    #     print(k, var.shape, var.op_type)

    input_dicts, output_dicts = MNN.expr.get_inputs_and_outputs(var_map)
    input_names = [n for n in input_dicts.keys()]
    output_names = [n for n in output_dicts.keys()]
    input_vars = [input_dicts[n] for n in input_names]
    output_vars = [output_dicts[n] for n in output_names]

    model = MNN.nn.load_module(input_vars, output_vars, True)
    return model