from steps.transform import transformer_fn


def test_tranform_fn_returns_object_with_correct_spec():
    # pylint: disable=assignment-from-none
    transformer = transformer_fn()
    # pylint: enable=assignment-from-none
    if transformer:
        assert callable(getattr(transformer, "fit", None))
        assert callable(getattr(transformer, "transform", None))
