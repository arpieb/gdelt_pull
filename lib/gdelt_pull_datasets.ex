defmodule GDELTPull.Datasets do
  @moduledoc """
  Documentation for GDELTPull.Datasets
  """
  @url_masterfilelist_en "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
  @url_masterfilelist_translation "http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt"
  @url_lastupdate_en "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
  @url_lastupdate_translation "http://data.gdeltproject.org/gdeltv2/lastupdate-translation.txt"

  ########################################
  # Public API
  ########################################

  @doc ~S"""
  Retrieve the GDELT 2.0 master download list from the data service
  """
  @spec get_masterfile_list(language::term) :: map
  def get_masterfile_list(:en) do
    get_download_list(@url_masterfilelist_en)
  end
  def get_masterfile_list(:translation) do
    get_download_list(@url_masterfilelist_translation)
  end

  @doc ~S"""
  Retrieve the GDELT 2.0 15-minute incremental download list from the data service
  """
  @spec get_lastupdate_list(language::term) :: map
  def get_lastupdate_list(:en) do
    get_download_list(@url_lastupdate_en)
  end
  def get_lastupdate_list(:translation) do
    get_download_list(@url_lastupdate_translation)
  end

  @doc ~S"""
  Pull the requested file list down from the URL and parse lines
  """
  @spec get_download_list(url::binary) :: map
  def get_download_list(url) when is_binary(url) do
    HTTPoison.get(url) |> process_download_list()
  end

  @doc ~S"""
  Download GDELT 2.0 data file from the captures map data to a destination path
  """
  @spec download_file(record::map, dest::binary) :: {:ok} | {:error, term}
  def download_file(record, dest) do
    dest_filename = Path.join(dest, record[:filename])
    case File.exists?(dest_filename) do
      :false -> HTTPoison.get(record[:url]) |> save_download(dest_filename)
      :true -> {:ok}
      _ -> {:error, "Unrecognized return from File.exists?/1"}
    end
  end

  @doc ~S"""
  Download a batch of GDELT 2.0 data files
  """
  @spec download_files(records::map, dest::binary, options::list) :: {:ok} | {:error, term}
  def download_files(records, dest), do: download_files(records, dest, [])
  def download_files([], _dest, _options), do: []
  def download_files(records, dest, options) do
    record = hd(records)
    url = record[:url]
    if Enum.member?(options, :verbose) do
      IO.puts("Downloading #{url}")
    end
    [{url, download_file(record, dest)} | download_files(tl(records), dest, options)]
  end

  ########################################
  # Private functions
  ########################################

  # Save download file
  defp save_download({:ok, %HTTPoison.Response{body: body}}, dest_filename) do
    File.write(dest_filename, body) |> format_return_tuple()
  end
  defp save_download(error, _dest_filename), do: error

  # Provide a mechanism to ensure correct return tuple
  defp format_return_tuple(retval) do
    case retval do
      :ok -> {:ok}
      :error -> {:error, "Unrecognized error"}
      _ -> retval
    end
  end

  # Process the list of download dataset records
  defp process_download_list({:ok, %HTTPoison.Response{body: body}}) do
    # Save to temp file
    list_filename = System.tmp_dir() |> Path.join("download_list.txt")
    File.write(list_filename, body)

    # Process lines in file as a stream
    File.stream!(list_filename)
    |> Enum.map(&parse_to_map/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.to_list()
  end
  defp process_download_list(error), do: error

  # Use regex to split up lines and identify which dataset it belongs to
  # (exported events, article mentions, or knowledge graph fragment)
  defp parse_to_map(line) do
    rex = ~R/(?<size>\d+)\s+(?<hash>[0-9a-f]+)\s+(?<url>(?:http|https):\/\/(?:[^\/]+\/)*(?<filename>.*(?<type>export|mentions|gkg).*))/
    Regex.named_captures(rex, line)
    |> keys_to_atoms()
    |> convert_values()
  end

  # Convert map keys from strings to atoms
  defp keys_to_atoms(:nil), do: :nil
  defp keys_to_atoms(captures) do
    for {key, val} <- captures, into: %{} do
      {String.to_atom(key), val}
    end
  end

  # Convert string values into correct types
  defp convert_values(:nil), do: :nil
  defp convert_values(captures) do
    captures
    |> Map.put(:type, String.to_atom(captures[:type]))
    |> Map.put(:size, String.to_integer(captures[:size]))
  end

end
