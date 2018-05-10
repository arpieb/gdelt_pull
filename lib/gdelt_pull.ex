defmodule GDELTPull do
  @moduledoc """
  Documentation for GDELTPull.
  """

  import GDELTPull.Datasets

  def main(args) do
    args
    |> parse_args()
    |> process_args()
  end

  defp parse_args(args) do
    {params, _, _} =  OptionParser.parse(args, switches: [help: :boolean, dest: :string])
    params
  end

  defp process_args([help: true]) do
    print_help_message()
  end

  defp process_args([dest: dest]) do
    case File.dir?(dest) do
      :true ->
        get_masterfile_list(:en)
        |> download_files(dest)
      _ -> IO.puts("#{dest} is not a directory")
    end
  end

  defp print_help_message() do
    IO.puts("Help!")
  end

end
