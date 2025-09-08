import pytest
from unittest.mock import MagicMock, patch, mock_open
from pipeline.logic.etl import ETLPipeline, ExecuteSqlFileStep, CopyDataStep, run_full_pipeline

@pytest.fixture
def mock_cursor():
    """Cria um mock do cursor do banco de dados."""
    return MagicMock()

def test_execute_sql_file_step(mock_cursor):
    """Testa se o passo de execução de SQL lê o arquivo e executa o comando."""
    sql_content = "SELECT 1;"
    # Mock para a função 'open' embutida, especificando o encoding
    with patch("builtins.open", mock_open(read_data=sql_content)) as mocked_file:
        step = ExecuteSqlFileStep(mock_cursor, "/fake/path/script.sql")
        step.execute()

        mocked_file.assert_called_once_with("/fake/path/script.sql", 'r', encoding='utf-8')
        mock_cursor.execute.assert_called_once_with(sql_content)

def test_copy_data_step(mock_cursor):
    """Testa se o passo de cópia de dados usa o contexto e chama copy_expert."""
    context = {'data_file_path': '/fake/data.txt'}
    copy_sql_content = "COPY table FROM STDIN;"
    data_content = "file_content"
    
    # Usar um side_effect para mockar a abertura de múltiplos arquivos com encodings corretos
    def open_side_effect(file_path, *args, **kwargs):
        if file_path == "/fake/copy.sql":
            return mock_open(read_data=copy_sql_content).return_value
        elif file_path == "/fake/data.txt":
            return mock_open(read_data=data_content).return_value
        return mock_open().return_value

    with patch("builtins.open", side_effect=open_side_effect):
        step = CopyDataStep(mock_cursor, "/fake/copy.sql")
        step.execute(context)

        mock_cursor.copy_expert.assert_called_once()
        args, kwargs = mock_cursor.copy_expert.call_args
        assert kwargs['sql'] == copy_sql_content

def test_copy_data_step_no_context_raises_error(mock_cursor):
    """Testa se um erro é lançado quando o caminho do arquivo não está no contexto."""
    step = CopyDataStep(mock_cursor, "/fake/copy.sql")
    with pytest.raises(ValueError, match="Caminho do arquivo de dados não encontrado"):
        step.execute({})

def test_etl_pipeline_runs_steps_in_order(mock_cursor):
    """Testa se o pipeline executa os passos na ordem correta."""
    pipeline = ETLPipeline(mock_cursor)
    
    # Mocks para os passos
    step1 = MagicMock()
    step2 = MagicMock()

    pipeline.steps = [step1, step2]
    pipeline.run()

    step1.execute.assert_called_once()
    step2.execute.assert_called_once()

@patch('pipeline.logic.etl.ETLPipeline')
@patch('pipeline.logic.etl.connection')
def test_run_full_pipeline(mock_connection, mock_ETLPipeline):
    """Testa a função orquestradora, garantindo que ela monta e executa o pipeline."""
    mock_pipeline_instance = mock_ETLPipeline.return_value
    
    success, _ = run_full_pipeline("/fake/data.txt")

    assert success is True
    mock_ETLPipeline.assert_called_once()
    mock_pipeline_instance.run.assert_called_once_with({'data_file_path': '/fake/data.txt'})
