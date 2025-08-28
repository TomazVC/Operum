class Municipio
{
    // CSV padrão: TOM;IBGE;Nome(TOM);Nome(IBGE);UF
    public string Tom { get; set; } = "";
    public string Ibge { get; set; } = "";
    public string NomeTom { get; set; } = "";
    public string NomeIbge { get; set; } = "";
    public string Uf { get; set; } = "";

    public string NomePreferido => !string.IsNullOrWhiteSpace(NomeIbge) ? NomeIbge : NomeTom;

    // Concatena TODOS os campos, para gerar o “material” do hash
    public string ToConcatenatedString()
        => $"{Tom};{Ibge};{NomeTom};{NomeIbge};{Uf}";
}
