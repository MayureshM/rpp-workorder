```json
[
    {
        "path": "ai-generated-tests/test_rejection.py",
        "content": "import pytest\nfrom rejection import process_rejection\n\ndef test_process_rejection():\n    # Arrange\n    event = {...}\n    expected_output = {...}\n    # Act\n    result = process_rejection(event, _)\n    # Assert\n    assert result == expected_output\n"
    },
    {
        "path": "ai-generated-tests/test_recon_approval.py",
        "content": "import pytest\nfrom recon_approval import process_recon_approval\n\ndef test_process_recon_approval():\n    # Arrange\n    event = {...}\n    expected_output = {...}\n    # Act\n    result = process_recon_approval(event, _)\n    # Assert\n    assert result == expected_output\n"
    },
    {
        "path": "ai-generated-tests/test_labor_category.py",
        "content": "import pytest\nfrom labor_category import find_labor_category\n\ndef test_find_labor_category():\n    # Arrange\n    event = {...}\n    expected_output = {...}\n    # Act\n    result = find_labor_category(event, _)\n    # Assert\n    assert result == expected_output\n"
    },
    {
        "path": "ai-generated-tests/test_recon_work_order.py",
        "content": "import pytest\nfrom recon_work_order import find\n\ndef test_find_recon_work_order():\n    # Arrange\n    event = {...}\n    expected_output = {...}\n    # Act\n    result = find(event, _)\n    # Assert\n    assert result == expected_output\n"
    },
    {
        "path": "ai-generated-tests/test_recon_service_status_ingest.py",
        "content": "import pytest\nfrom recon_service_status_ingest import process_stream\n\ndef test_process_stream():\n    # Arrange\n    event = {...}\n    expected_output = {...}\n    # Act\n    result = process_stream(event, _)\n    # Assert\n    assert result == expected_output\n"
    },
    {
        "path": "ai-generated-tests/test_enhanced_notes.py",
        "content": "import pytest\nfrom enhanced_notes import handler\n\ndef test_handler():\n    # Arrange\n    event = {...}\n    expected_output = {...}\n    # Act\n    result = handler(event, _)\n    # Assert\n    assert result == expected_output\n"
    },
    {
        "path": "ai-generated-tests/test_recon_retail_inspection.py",
        "content": "import pytest\nfrom recon_retail_inspection import process_record\n\ndef test_process_record():\n    # Arrange\n    record = {...}\n    expected_output = {...}\n    # Act\n    result = process_record(record)\n    # Assert\n    assert result == expected_output\n"
    },
    {
        "path": "ai-generated-tests/test_work_credit.py",
        "content": "import pytest\nfrom work_credit import process_queue\n\ndef test_process_queue():\n    # Arrange\n    event = {...}\n    expected_output = {...}\n    # Act\n    result = process_queue(event, _)\n    # Assert\n    assert result == expected_output\n"
    },
    {
        "path": "ai-generated-tests/test_workorder.py",
        "content": "import pytest\nfrom workorder import find\n\ndef test_find_workorder():\n    # Arrange\n    event = {...}\n    expected_output = {...}\n    # Act\n    result = find(event, _)\n    # Assert\n    assert result == expected_output\n"
    },
    {
        "path": "ai-generated-tests/test_order_image.py",
        "content": "import pytest\nfrom order_image import process_order_image\n\ndef test_process_order_image():\n    # Arrange\n    event = {...}\n    expected_output = {...}\n    # Act\n    result = process_order_image(event, _)\n    # Assert\n    assert result == expected_output\n"
    }
]
```