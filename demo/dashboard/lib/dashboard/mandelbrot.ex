defmodule Dashboard.Mandelbrot do
  @moduledoc """
  The same tile renderer as the Rust node, in Elixir, for the side-by-side
  comparison. Plain floating point loops: this is exactly the kind of work
  the BEAM is not built for, and the reason the Rust node exists.
  """

  @doc "Render a tile request map to `%{png: binary, ms: float, iterations: n}`."
  def tile(%{x0: x0, y0: y0, x1: x1, y1: y1, size: n, max_iter: max_iter} = req, progress \\ fn _ -> :ok end) do
    t0 = System.monotonic_time(:microsecond)

    {rows, iterations} =
      Enum.map_reduce(0..(n - 1), 0, fn py, acc ->
        ci = y0 + (y1 - y0) * (py + 0.5) / n

        {pixels, iters} =
          Enum.map_reduce(0..(n - 1), 0, fn px, acc ->
            cr = x0 + (x1 - x0) * (px + 0.5) / n
            {i, zr, zi} = escape(cr, ci, 0.0, 0.0, 0, max_iter)
            {pixel(i, zr, zi, max_iter), acc + i}
          end)

        progress.(py + 1)
        {IO.iodata_to_binary([0 | pixels]), acc + iters}
      end)

    ms = (System.monotonic_time(:microsecond) - t0) / 1000
    %{id: req[:id], node: node(), png: png(rows, n), ms: ms, iterations: iterations}
  end

  defp escape(_cr, _ci, zr, zi, i, max) when i >= max or zr * zr + zi * zi > 4.0, do: {i, zr, zi}

  defp escape(cr, ci, zr, zi, i, max) do
    escape(cr, ci, zr * zr - zi * zi + cr, 2.0 * zr * zi + ci, i + 1, max)
  end

  defp pixel(i, _zr, _zi, max) when i >= max, do: <<0, 0, 0>>

  defp pixel(i, zr, zi, _max) do
    mag = max(:math.sqrt(zr * zr + zi * zi), 1.0000001)
    nu = i + 1.0 - :math.log(:math.log(mag)) / :math.log(2.0)
    t = nu / 64.0
    t = t - Float.floor(t)
    <<chan(t, 0.0), chan(t, 0.33), chan(t, 0.67)>>
  end

  defp chan(t, d) do
    v = (0.5 + 0.5 * :math.cos(6.283185 * (t + d))) * 255.0
    v |> max(0.0) |> min(255.0) |> trunc()
  end

  # A minimal PNG writer: 8-bit RGB, one zlib stream, no filtering.
  defp png(rows, n) do
    ihdr = <<n::32, n::32, 8, 2, 0, 0, 0>>
    idat = :zlib.compress(IO.iodata_to_binary(rows))
    IO.iodata_to_binary([<<137, 80, 78, 71, 13, 10, 26, 10>>, chunk("IHDR", ihdr), chunk("IDAT", idat), chunk("IEND", "")])
  end

  defp chunk(type, data) do
    crc = :erlang.crc32(type <> data)
    <<byte_size(data)::32, type::binary, data::binary, crc::32>>
  end
end
